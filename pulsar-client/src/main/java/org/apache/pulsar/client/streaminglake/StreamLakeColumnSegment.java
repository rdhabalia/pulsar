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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * A <b>column-oriented segment</b> for one indexed column of a data ledger: a per-page array of that
 * column's stats, so a scan prunes to the exact candidate pages from this one entry (no per-page
 * footer read once a ledger is segmented). Position {@code i} in the array is the {@code i}-th page of
 * the data ledger; the segment's page directory maps position -&gt; data entryId.
 *
 * <p>Per page it keeps the order-preserving {@code [min, max]}; for text/bytes columns it also keeps a
 * per-page <b>bloom</b> (converted from the page's exact set) so equality/IN prunes the exact page.
 * When the per-page array would exceed {@code maxBytes} (e.g. a high-cardinality {@code email} whose
 * per-page blooms balloon), the column <b>collapses</b> to a single whole-segment stat: union min/max,
 * plus one bloom from the union of the pages' exact sets when they were all low-cardinality (else
 * min/max only). Never a false negative -- surviving pages are still exactly row-filtered on read.
 */
public final class StreamLakeColumnSegment {

    private static final Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;
    private static final int FLAG_MINMAX = 0x1;
    private static final int FLAG_BLOOM = 0x2;

    private final int columnIndex;
    private final StreamLakeType type;
    private final boolean collapsed;
    private final int numPages;
    // Per-page arrays (length numPages) when !collapsed; a null cell means the page had no values.
    private final byte[][] pageMin;
    private final byte[][] pageMax;
    private final byte[][] pageBloom; // per-page membership bloom (text only), else null
    // The single whole-segment stat when collapsed.
    private final byte[] cMin;
    private final byte[] cMax;
    private final byte[] cBloom;

    private StreamLakeColumnSegment(int columnIndex, StreamLakeType type, boolean collapsed, int numPages,
            byte[][] pageMin, byte[][] pageMax, byte[][] pageBloom, byte[] cMin, byte[] cMax, byte[] cBloom) {
        this.columnIndex = columnIndex;
        this.type = type;
        this.collapsed = collapsed;
        this.numPages = numPages;
        this.pageMin = pageMin;
        this.pageMax = pageMax;
        this.pageBloom = pageBloom;
        this.cMin = cMin;
        this.cMax = cMax;
        this.cBloom = cBloom;
    }

    public int columnIndex() {
        return columnIndex;
    }

    public boolean collapsed() {
        return collapsed;
    }

    public int numPages() {
        return numPages;
    }

    /**
     * Build a column segment from the per-page stats of a data ledger (position i = page i). Text
     * columns store a per-page bloom (built from the page's exact set, or reusing its bloom); the
     * column collapses to one whole-segment stat once the per-page array would exceed {@code maxBytes}.
     */
    public static StreamLakeColumnSegment build(int columnIndex, StreamLakeType type,
            List<StreamLakeBatchStats.ColumnStats> perPage, long maxBytes, double bloomFpp) {
        int numPages = perPage.size();
        boolean textLike = type == StreamLakeType.STRING || type == StreamLakeType.BYTES;
        byte[][] pMin = new byte[numPages][];
        byte[][] pMax = new byte[numPages][];
        byte[][] pBloom = new byte[numPages][];
        long size = 0;
        boolean allExactSets = true; // among pages that carry values -> can rebuild a union bloom
        for (int i = 0; i < numPages; i++) {
            StreamLakeBatchStats.ColumnStats cs = perPage.get(i);
            if (cs == null || cs.min == null) {
                continue; // all-null page for this column
            }
            pMin[i] = cs.min;
            pMax[i] = cs.max;
            size += cs.min.length + cs.max.length + 12;
            if (cs.exactSet == null) {
                allExactSets = false;
            }
            if (textLike) {
                if (cs.exactSet != null) {
                    pBloom[i] = StreamLakeBloom.build(Arrays.asList(cs.exactSet), bloomFpp).encode();
                } else if (cs.bloom != null) {
                    pBloom[i] = cs.bloom;
                }
                if (pBloom[i] != null) {
                    size += pBloom[i].length + 4;
                }
            }
        }
        if (size <= maxBytes) {
            return new StreamLakeColumnSegment(columnIndex, type, false, numPages,
                    pMin, pMax, pBloom, null, null, null);
        }
        // Collapse: one whole-segment stat (union min/max; union-of-sets bloom when recoverable).
        byte[] min = null;
        byte[] max = null;
        TreeSet<byte[]> union = new TreeSet<>(UNSIGNED);
        for (StreamLakeBatchStats.ColumnStats cs : perPage) {
            if (cs == null || cs.min == null) {
                continue;
            }
            min = (min == null || UNSIGNED.compare(cs.min, min) < 0) ? cs.min : min;
            max = (max == null || UNSIGNED.compare(cs.max, max) > 0) ? cs.max : max;
            if (cs.exactSet != null) {
                union.addAll(Arrays.asList(cs.exactSet));
            }
        }
        byte[] bloom = (allExactSets && !union.isEmpty())
                ? StreamLakeBloom.build(union, bloomFpp).encode() : null;
        return new StreamLakeColumnSegment(columnIndex, type, true, numPages,
                null, null, null, min, max, bloom);
    }

    /**
     * Which page positions might match {@code cp}: {@code out[i]} is true if page {@code i} could
     * contain a matching row. For a collapsed column every position shares the whole-segment verdict.
     */
    public boolean[] candidatePositions(StreamLakeScanPredicate.ColumnPredicate cp) {
        boolean[] out = new boolean[numPages];
        if (collapsed) {
            boolean match = matches(cMin, cMax, cBloom, cp);
            Arrays.fill(out, match);
            return out;
        }
        for (int i = 0; i < numPages; i++) {
            out[i] = matches(pageMin[i], pageMax[i], pageBloom[i], cp);
        }
        return out;
    }

    private boolean matches(byte[] min, byte[] max, byte[] bloom, StreamLakeScanPredicate.ColumnPredicate cp) {
        // Reconstruct a single-page ColumnStats and reuse the shared prune logic (min/max range +
        // bloom membership). A null min (all-null page) can't be excluded -> conservatively matches.
        StreamLakeBatchStats.ColumnStats cs = new StreamLakeBatchStats.ColumnStats(
                columnIndex, type, min, max, min == null ? 0 : 1, null, bloom);
        return cp.matches(cs);
    }

    public byte[] encode() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(columnIndex);
            out.writeByte(type.ordinal());
            out.writeBoolean(collapsed);
            out.writeInt(numPages);
            if (collapsed) {
                writePageStat(out, cMin, cMax, cBloom);
            } else {
                for (int i = 0; i < numPages; i++) {
                    writePageStat(out, pageMin[i], pageMax[i], pageBloom[i]);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake column segment encode failed", e);
        }
        return bos.toByteArray();
    }

    public static StreamLakeColumnSegment decode(byte[] blob) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(blob))) {
            int columnIndex = in.readInt();
            StreamLakeType type = StreamLakeType.values()[in.readByte() & 0xFF];
            boolean collapsed = in.readBoolean();
            int numPages = in.readInt();
            if (collapsed) {
                byte[][] one = readPageStat(in);
                return new StreamLakeColumnSegment(columnIndex, type, true, numPages,
                        null, null, null, one[0], one[1], one[2]);
            }
            byte[][] pMin = new byte[numPages][];
            byte[][] pMax = new byte[numPages][];
            byte[][] pBloom = new byte[numPages][];
            for (int i = 0; i < numPages; i++) {
                byte[][] stat = readPageStat(in);
                pMin[i] = stat[0];
                pMax[i] = stat[1];
                pBloom[i] = stat[2];
            }
            return new StreamLakeColumnSegment(columnIndex, type, false, numPages,
                    pMin, pMax, pBloom, null, null, null);
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake column segment decode failed", e);
        }
    }

    private static void writePageStat(DataOutputStream out, byte[] min, byte[] max, byte[] bloom)
            throws IOException {
        int flags = (min != null ? FLAG_MINMAX : 0) | (bloom != null ? FLAG_BLOOM : 0);
        out.writeByte(flags);
        if (min != null) {
            out.writeInt(min.length);
            out.write(min);
            out.writeInt(max.length);
            out.write(max);
        }
        if (bloom != null) {
            out.writeInt(bloom.length);
            out.write(bloom);
        }
    }

    private static byte[][] readPageStat(DataInputStream in) throws IOException {
        int flags = in.readByte() & 0xFF;
        byte[] min = null;
        byte[] max = null;
        byte[] bloom = null;
        if ((flags & FLAG_MINMAX) != 0) {
            min = new byte[in.readInt()];
            in.readFully(min);
            max = new byte[in.readInt()];
            in.readFully(max);
        }
        if ((flags & FLAG_BLOOM) != 0) {
            bloom = new byte[in.readInt()];
            in.readFully(bloom);
        }
        return new byte[][]{min, max, bloom};
    }
}
