/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.streaminglake;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Per-batch pruning statistics for a StreamLake columnar batch: for each indexed column, an
 * order-preserving min/max plus either a low-cardinality exact set or a high-cardinality bloom.
 * Serialized to the binary "stats footer" the broker slices off a message (without parsing Arrow)
 * and appends to the page-index ledger.
 */
public final class StreamLakeBatchStats {

    private static final byte[] MAGIC = {'S', 'L', 'S', '1'};
    private static final byte VERSION = 1;
    private static final Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;

    /** Per-column stats. {@code min/max} are null only when every value in the batch was null. */
    public static final class ColumnStats {
        final int columnIndex;
        final StreamLakeType type;
        final byte[] min;
        final byte[] max;
        final int distinctCount;
        final byte[][] exactSet; // sorted order-preserving values, or null for high-cardinality
        final byte[] bloom;      // encoded StreamLakeBloom, or null for low-cardinality

        ColumnStats(int columnIndex, StreamLakeType type, byte[] min, byte[] max, int distinctCount,
                    byte[][] exactSet, byte[] bloom) {
            this.columnIndex = columnIndex;
            this.type = type;
            this.min = min;
            this.max = max;
            this.distinctCount = distinctCount;
            this.exactSet = exactSet;
            this.bloom = bloom;
        }

        public int columnIndex() {
            return columnIndex;
        }

        public StreamLakeType type() {
            return type;
        }

        public byte[] min() {
            return min;
        }

        public byte[] max() {
            return max;
        }

        public int distinctCount() {
            return distinctCount;
        }

        /** True if the query range [{@code lo},{@code hi}] (order-preserving) overlaps [min,max]. */
        public boolean overlaps(byte[] lo, byte[] hi) {
            if (min == null) {
                return false; // all-null column can't match a value predicate
            }
            return UNSIGNED.compare(lo, max) <= 0 && UNSIGNED.compare(hi, min) >= 0;
        }

        /** Membership test for an order-preserving-encoded value (exact when a set is present). */
        public boolean mightContain(byte[] encodedValue) {
            if (exactSet != null) {
                return Arrays.binarySearch(exactSet, encodedValue, UNSIGNED) >= 0;
            }
            if (bloom != null) {
                return StreamLakeBloom.decode(bloom).mightContain(encodedValue);
            }
            return true; // no membership filter for this column
        }
    }

    private final List<ColumnStats> columns;

    StreamLakeBatchStats(List<ColumnStats> columns) {
        this.columns = columns;
    }

    public List<ColumnStats> columns() {
        return columns;
    }

    public ColumnStats column(int columnIndex) {
        for (ColumnStats cs : columns) {
            if (cs.columnIndex == columnIndex) {
                return cs;
            }
        }
        return null;
    }

    /** Serialize to the binary stats-footer format. */
    public byte[] encode() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.write(MAGIC);
            out.writeByte(VERSION);
            out.writeInt(columns.size());
            for (ColumnStats cs : columns) {
                out.writeInt(cs.columnIndex);
                out.writeByte(cs.type.ordinal());
                int flags = (cs.min != null ? 1 : 0) | (cs.exactSet != null ? 2 : 0)
                        | (cs.bloom != null ? 4 : 0);
                out.writeByte(flags);
                out.writeInt(cs.distinctCount);
                if (cs.min != null) {
                    writeBlob(out, cs.min);
                    writeBlob(out, cs.max);
                }
                if (cs.exactSet != null) {
                    out.writeInt(cs.exactSet.length);
                    for (byte[] v : cs.exactSet) {
                        writeBlob(out, v);
                    }
                }
                if (cs.bloom != null) {
                    writeBlob(out, cs.bloom);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake stats encode failed", e);
        }
        return bos.toByteArray();
    }

    /** Parse a stats footer produced by {@link #encode()}. */
    public static StreamLakeBatchStats decode(byte[] blob) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(blob))) {
            byte[] magic = new byte[MAGIC.length];
            in.readFully(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IllegalArgumentException("Not a StreamLake stats footer");
            }
            in.readByte(); // version (reserved)
            int n = in.readInt();
            List<ColumnStats> cols = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                int columnIndex = in.readInt();
                StreamLakeType type = StreamLakeType.values()[in.readByte() & 0xFF];
                int flags = in.readByte() & 0xFF;
                int distinctCount = in.readInt();
                byte[] min = null;
                byte[] max = null;
                byte[][] set = null;
                byte[] bloom = null;
                if ((flags & 1) != 0) {
                    min = readBlob(in);
                    max = readBlob(in);
                }
                if ((flags & 2) != 0) {
                    int count = in.readInt();
                    set = new byte[count][];
                    for (int s = 0; s < count; s++) {
                        set[s] = readBlob(in);
                    }
                }
                if ((flags & 4) != 0) {
                    bloom = readBlob(in);
                }
                cols.add(new ColumnStats(columnIndex, type, min, max, distinctCount, set, bloom));
            }
            return new StreamLakeBatchStats(cols);
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake stats decode failed", e);
        }
    }

    private static void writeBlob(DataOutputStream out, byte[] b) throws IOException {
        out.writeInt(b.length);
        out.write(b);
    }

    private static byte[] readBlob(DataInputStream in) throws IOException {
        byte[] b = new byte[in.readInt()];
        in.readFully(b);
        return b;
    }
}
