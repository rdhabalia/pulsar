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
package org.apache.pulsar.broker.service.streaminglake;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.bookie.storage.ldb.BloomFilter;

/**
 * A StreamLake page: N messages packed into one bookie entry, column-major, carved into
 * <b>granules</b> (sub-page row groups) each with a per-column <b>zone map</b> — min/max plus a
 * value bloom. The bookie still prunes whole pages by the page-level index; the broker then prunes
 * <i>granules within a surviving page</i> using these zone maps, so selective decode skips a
 * granule's column data and per-row work entirely instead of scanning every row.
 *
 * <pre>
 *   header(44): magic 'SLB1' | version 1 | flags | numMessages | numCols | minDate | maxDate
 *               | granuleSize | numGranules | zoneMapOffset | payloadIndexOffset
 *   column directory:  numCols x [ columnId(2) type(1) dataOffset(4) ]
 *   zone maps:         per column, per granule [ min(8) max(8) bloomLen(4) bloomBytes ]
 *   column data:       per column, numMessages values (INT=4, LONG=8)   (column-major)
 *   payload index:     (numMessages+1) x offset(4)
 *   payloads:          each message's headersAndPayload
 * </pre>
 */
public final class StreamLakeBatchPage {

    private static final int MAGIC = 0x534C4231; // 'S','L','B','1'
    private static final byte VERSION = 1;
    public static final byte FLAG_COLUMNAR = 0x1;
    public static final byte FLAG_VORTEX = 0x2;

    public static final byte TYPE_INT = 0;
    public static final byte TYPE_LONG = 1;

    private static final int BLOOM_BITS_PER_ELEMENT = 10;

    // header field offsets
    private static final int OFF_NUM_MESSAGES = 6;
    private static final int OFF_NUM_COLS = 10;
    private static final int OFF_MIN_DATE = 12;
    private static final int OFF_MAX_DATE = 20;
    private static final int OFF_GRANULE_SIZE = 28;
    private static final int OFF_NUM_GRANULES = 32;
    private static final int OFF_ZONEMAP_OFFSET = 36;
    private static final int OFF_PAYLOAD_INDEX_OFFSET = 40;
    private static final int HEADER = 44;
    private static final int DIR_ENTRY = 7; // columnId(2) + type(1) + dataOffset(4)

    private StreamLakeBatchPage() {
    }

    /** Per-granule, per-column statistics used to skip granules during selective decode. */
    public static final class GranuleStat {
        public final long min;
        public final long max;
        public final byte[] bloom;

        GranuleStat(long min, long max, byte[] bloom) {
            this.min = min;
            this.max = max;
            this.bloom = bloom;
        }
    }

    public static boolean isPage(ByteBuf buf) {
        return buf.readableBytes() >= 4 && buf.getInt(buf.readerIndex()) == MAGIC;
    }

    /**
     * Encode a page.
     *
     * @param columnValues columnValues[c][i] = column c's value for message i (INT in the low 32 bits)
     * @param granuleSize  rows per granule (clamped to ≥ 1)
     */
    public static ByteBuf encode(List<ByteBuf> messages, int[] columnIds, byte[] columnTypes,
                                 long[][] columnValues, long minDate, long maxDate, int granuleSize) {
        int n = messages.size();
        int numCols = columnIds.length;
        int g = Math.max(1, granuleSize);
        int numGranules = Math.max(1, (n + g - 1) / g);

        // build per-column, per-granule min/max + bloom
        long[][] gMin = new long[numCols][numGranules];
        long[][] gMax = new long[numCols][numGranules];
        byte[][][] gBloom = new byte[numCols][numGranules][];
        for (int c = 0; c < numCols; c++) {
            for (int gi = 0; gi < numGranules; gi++) {
                int from = gi * g;
                int to = Math.min(from + g, n);
                long mn = Long.MAX_VALUE;
                long mx = Long.MIN_VALUE;
                List<byte[]> enc = new ArrayList<>(to - from);
                for (int i = from; i < to; i++) {
                    long v = columnValues[c][i];
                    mn = Math.min(mn, v);
                    mx = Math.max(mx, v);
                    enc.add(columnTypes[c] == TYPE_LONG ? encLong(v) : encInt((int) v));
                }
                gMin[c][gi] = mn;
                gMax[c][gi] = mx;
                gBloom[c][gi] = BloomFilter.build(enc, BLOOM_BITS_PER_ELEMENT);
            }
        }

        // layout
        int dirStart = HEADER;
        int zoneMapStart = dirStart + numCols * DIR_ENTRY;
        int cursor = zoneMapStart;
        for (int c = 0; c < numCols; c++) {
            for (int gi = 0; gi < numGranules; gi++) {
                cursor += 8 + 8 + 4 + gBloom[c][gi].length;
            }
        }
        int colDataStart = cursor;
        int[] colOffset = new int[numCols];
        for (int c = 0; c < numCols; c++) {
            colOffset[c] = cursor;
            cursor += n * (columnTypes[c] == TYPE_LONG ? 8 : 4);
        }
        int payloadIndexOffset = cursor;
        cursor += (n + 1) * 4;
        int payloadDataStart = cursor;
        int[] payloadOffsets = new int[n + 1];
        int p = payloadDataStart;
        for (int i = 0; i < n; i++) {
            payloadOffsets[i] = p;
            p += messages.get(i).readableBytes();
        }
        payloadOffsets[n] = p;
        int total = p;

        ByteBuf out = Unpooled.buffer(total, total);
        out.writeInt(MAGIC);
        out.writeByte(VERSION);
        out.writeByte(FLAG_COLUMNAR);
        out.writeInt(n);
        out.writeShort(numCols);
        out.writeLong(minDate);
        out.writeLong(maxDate);
        out.writeInt(g);
        out.writeInt(numGranules);
        out.writeInt(zoneMapStart);
        out.writeInt(payloadIndexOffset);
        // directory
        for (int c = 0; c < numCols; c++) {
            out.writeShort(columnIds[c]);
            out.writeByte(columnTypes[c]);
            out.writeInt(colOffset[c]);
        }
        // zone maps (column-major: all granules of col 0, then col 1, ...)
        for (int c = 0; c < numCols; c++) {
            for (int gi = 0; gi < numGranules; gi++) {
                out.writeLong(gMin[c][gi]);
                out.writeLong(gMax[c][gi]);
                out.writeInt(gBloom[c][gi].length);
                out.writeBytes(gBloom[c][gi]);
            }
        }
        // column data
        for (int c = 0; c < numCols; c++) {
            for (int i = 0; i < n; i++) {
                if (columnTypes[c] == TYPE_LONG) {
                    out.writeLong(columnValues[c][i]);
                } else {
                    out.writeInt((int) columnValues[c][i]);
                }
            }
        }
        // payload index + payloads
        for (int i = 0; i <= n; i++) {
            out.writeInt(payloadOffsets[i]);
        }
        for (ByteBuf m : messages) {
            out.writeBytes(m, m.readerIndex(), m.readableBytes());
        }
        return out;
    }

    public static int messageCount(ByteBuf page) {
        return page.getInt(page.readerIndex() + OFF_NUM_MESSAGES);
    }

    public static long minDate(ByteBuf page) {
        return page.getLong(page.readerIndex() + OFF_MIN_DATE);
    }

    public static long maxDate(ByteBuf page) {
        return page.getLong(page.readerIndex() + OFF_MAX_DATE);
    }

    public static int granuleSize(ByteBuf page) {
        return page.getInt(page.readerIndex() + OFF_GRANULE_SIZE);
    }

    public static int granuleCount(ByteBuf page) {
        return page.getInt(page.readerIndex() + OFF_NUM_GRANULES);
    }

    /** Decode the per-column granule zone maps: columnId -&gt; one GranuleStat per granule. */
    public static Map<Short, GranuleStat[]> decodeZoneMaps(ByteBuf page) {
        int base = page.readerIndex();
        int numCols = page.getShort(base + OFF_NUM_COLS);
        int numGranules = page.getInt(base + OFF_NUM_GRANULES);
        int zoneMapOffset = page.getInt(base + OFF_ZONEMAP_OFFSET);
        // columnId per directory position
        short[] colIds = new short[numCols];
        for (int c = 0; c < numCols; c++) {
            colIds[c] = page.getShort(base + HEADER + c * DIR_ENTRY);
        }
        Map<Short, GranuleStat[]> out = new HashMap<>();
        int off = base + zoneMapOffset;
        for (int c = 0; c < numCols; c++) {
            GranuleStat[] stats = new GranuleStat[numGranules];
            for (int gi = 0; gi < numGranules; gi++) {
                long mn = page.getLong(off);
                long mx = page.getLong(off + 8);
                int len = page.getInt(off + 16);
                byte[] bloom = new byte[len];
                page.getBytes(off + 20, bloom);
                stats[gi] = new GranuleStat(mn, mx, bloom);
                off += 20 + len;
            }
            out.put(colIds[c], stats);
        }
        return out;
    }

    /** Read column {@code columnId}'s values for rows [{@code fromRow}, {@code toRow}). */
    public static long[] readColumnRange(ByteBuf page, int columnId, int fromRow, int toRow) {
        int base = page.readerIndex();
        int numCols = page.getShort(base + OFF_NUM_COLS);
        int dir = base + HEADER;
        for (int c = 0; c < numCols; c++) {
            int entry = dir + c * DIR_ENTRY;
            if ((page.getShort(entry) & 0xFFFF) == columnId) {
                byte type = page.getByte(entry + 2);
                int dataOffset = page.getInt(entry + 3);
                int origin = base + dataOffset;
                long[] values = new long[toRow - fromRow];
                for (int i = fromRow; i < toRow; i++) {
                    values[i - fromRow] = type == TYPE_LONG
                            ? page.getLong(origin + i * 8) : page.getInt(origin + i * 4);
                }
                return values;
            }
        }
        throw new IllegalArgumentException("column not found: " + columnId);
    }

    /** Read all of column {@code columnId}'s values. */
    public static long[] readColumn(ByteBuf page, int columnId) {
        return readColumnRange(page, columnId, 0, messageCount(page));
    }

    /** Materialize a single message (its headersAndPayload) by row index. */
    public static ByteBuf messageAt(ByteBuf page, int index) {
        int base = page.readerIndex();
        int payloadIndexOffset = page.getInt(base + OFF_PAYLOAD_INDEX_OFFSET);
        int start = page.getInt(base + payloadIndexOffset + index * 4);
        int end = page.getInt(base + payloadIndexOffset + (index + 1) * 4);
        return page.retainedSlice(base + start, end - start);
    }

    /** Materialize every message (for full-batch transcoding to consumers). */
    public static List<ByteBuf> decode(ByteBuf page) {
        int n = messageCount(page);
        List<ByteBuf> messages = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            messages.add(messageAt(page, i));
        }
        return messages;
    }

    /** Order-preserving encodings — must match StreamLakePageScan.encodeKey / StreamLakeRangeBuilder. */
    private static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    private static byte[] encLong(long v) {
        long u = v ^ 0x8000000000000000L;
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) {
            b[i] = (byte) u;
            u >>>= 8;
        }
        return b;
    }
}
