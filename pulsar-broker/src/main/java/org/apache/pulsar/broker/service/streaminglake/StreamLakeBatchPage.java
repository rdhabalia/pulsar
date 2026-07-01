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
import java.util.TreeSet;
import org.apache.bookkeeper.bookie.storage.ldb.BloomFilter;

/**
 * A StreamLake page: N messages packed into one bookie entry, column-major, carved into
 * <b>granules</b> (sub-page row groups) each with a per-column <b>zone map</b>. A zone map carries
 * min/max, a value bloom, and — for low-cardinality columns — an <b>exact value set</b> (ClickHouse
 * {@code set(N)}) so equality/IN predicates prune a granule with no false positives even when the
 * value lies inside [min,max].
 *
 * <p>When a {@code sortColumnId} is configured the page additionally stores a <b>sparse primary
 * index</b>: a permutation of the rows by the sort key plus per-(sorted-)granule marks. The page's
 * <b>rows are NOT reordered</b> — payloads, column data and the payload index stay in <b>publish
 * order</b>, so ordinary consumers receive messages in the original order (the transcoder never reads
 * the sort index). Only the scan uses it, binary-searching the marks to a tiny granule window and
 * then dereferencing the permutation to the matching physical rows.
 *
 * <pre>
 *   header(50): magic 'SLB1' | version 1 | flags | numMessages | numCols | minDate | maxDate
 *               | granuleSize | numGranules | zoneMapOffset | payloadIndexOffset | sortColumnId
 *               | sortIndexOffset
 *   column directory:  numCols x [ columnId(2) type(1) codecId(1) dataOffset(4) encodedLen(4) ]
 *   zone maps:         per column, per granule
 *                        [ min(8) max(8) bloomLen(4) bloomBytes setCount(4) setVals(8 x setCount) ]
 *                        setCount == -1 means "no exact set" (cardinality above the cap)
 *   sort index:        (iff FLAG_SORTED) numGranules x [ min(8) max(8) ] then perm(numMessages x 4)
 *   column data:       per column, a {@link StreamLakeColumnCodec} block (raw fixed-stride when
 *                        compression is off; FLAG_VORTEX is set when any column is compressed)
 *   payload index:     (numMessages+1) x offset(4)                      (publish order)
 *   payloads:          each message's headersAndPayload                 (publish order)
 * </pre>
 */
public final class StreamLakeBatchPage {

    private static final int MAGIC = 0x534C4231; // 'S','L','B','1'
    private static final byte VERSION = 1;
    public static final byte FLAG_COLUMNAR = 0x1;
    public static final byte FLAG_VORTEX = 0x2;
    public static final byte FLAG_SORTED = 0x4;

    public static final byte TYPE_INT = 0;
    public static final byte TYPE_LONG = 1;

    private static final int BLOOM_BITS_PER_ELEMENT = 10;

    // header field offsets
    private static final int OFF_FLAGS = 5;
    private static final int OFF_NUM_MESSAGES = 6;
    private static final int OFF_NUM_COLS = 10;
    private static final int OFF_MIN_DATE = 12;
    private static final int OFF_MAX_DATE = 20;
    private static final int OFF_GRANULE_SIZE = 28;
    private static final int OFF_NUM_GRANULES = 32;
    private static final int OFF_ZONEMAP_OFFSET = 36;
    private static final int OFF_PAYLOAD_INDEX_OFFSET = 40;
    private static final int OFF_SORT_COL = 44;
    private static final int OFF_SORTINDEX_OFFSET = 46;
    private static final int HEADER = 50;
    // columnId(2) + type(1) + codecId(1) + dataOffset(4) + encodedLen(4)
    private static final int DIR_ENTRY = 12;
    private static final int DIR_CODEC = 3;   // codecId offset within a directory entry
    private static final int DIR_OFFSET = 4;  // dataOffset offset within a directory entry
    private static final int DIR_ENC_LEN = 8; // encodedLen offset within a directory entry

    private StreamLakeBatchPage() {
    }

    /** Per-granule, per-column statistics used to skip granules during selective decode. */
    public static final class GranuleStat {
        public final long min;
        public final long max;
        public final byte[] bloom;
        /** Sorted distinct values for this granule, or {@code null} if the cardinality cap was exceeded. */
        public final long[] set;

        GranuleStat(long min, long max, byte[] bloom, long[] set) {
            this.min = min;
            this.max = max;
            this.bloom = bloom;
            this.set = set;
        }

        /** Exact membership test; only valid when {@link #set} is present. */
        public boolean setContains(long v) {
            int lo = 0;
            int hi = set.length - 1;
            while (lo <= hi) {
                int mid = (lo + hi) >>> 1;
                if (set[mid] < v) {
                    lo = mid + 1;
                } else if (set[mid] > v) {
                    hi = mid - 1;
                } else {
                    return true;
                }
            }
            return false;
        }
    }

    public static boolean isPage(ByteBuf buf) {
        return buf.readableBytes() >= 4 && buf.getInt(buf.readerIndex()) == MAGIC;
    }

    /**
     * Encode a page.
     *
     * @param columnValues     columnValues[c][i] = column c's value for message i (INT in low 32 bits)
     * @param granuleSize      rows per granule (clamped to ≥ 1)
     * @param sortColumnId     sort rows by this column so granule marks form a sparse index (0 = none)
     * @param setMaxCardinality store an exact value set per granule up to this distinct count (else bloom only)
     * @param compress         compress each column block with the smallest lossless integer codec
     */
    public static ByteBuf encode(List<ByteBuf> messages, int[] columnIds, byte[] columnTypes,
                                 long[][] columnValues, long minDate, long maxDate, int granuleSize,
                                 int sortColumnId, int setMaxCardinality, boolean compress) {
        int n = messages.size();
        int numCols = columnIds.length;
        int g = Math.max(1, granuleSize);
        int numGranules = Math.max(1, (n + g - 1) / g);

        // optional sparse primary index: compute the sort permutation of the rows by the sort key,
        // but DO NOT reorder the page. Payloads, column data and the payload index stay in publish
        // order so ordinary consumer delivery is unchanged; the permutation + per-(sorted-)granule
        // marks are written as a side index that only the scan reads.
        int sortColIdx = -1;
        if (sortColumnId != 0) {
            for (int c = 0; c < numCols; c++) {
                if (columnIds[c] == sortColumnId) {
                    sortColIdx = c;
                    break;
                }
            }
        }
        boolean hasSortIndex = sortColIdx >= 0;
        int[] sortPerm = null;       // sortPerm[k] = physical row of the k-th smallest sort-key value
        long[] sortMarkMin = null;   // per logical (sorted) granule, monotonic non-decreasing
        long[] sortMarkMax = null;
        if (hasSortIndex) {
            final int sc = sortColIdx;
            final long[][] cv = columnValues;
            Integer[] perm = new Integer[n];
            for (int i = 0; i < n; i++) {
                perm[i] = i;
            }
            java.util.Arrays.sort(perm, (a, b) -> Long.compare(cv[sc][a], cv[sc][b]));
            sortPerm = new int[n];
            for (int i = 0; i < n; i++) {
                sortPerm[i] = perm[i];
            }
            sortMarkMin = new long[numGranules];
            sortMarkMax = new long[numGranules];
            for (int k = 0; k < numGranules; k++) {
                int from = k * g;
                int to = Math.min(from + g, n);
                sortMarkMin[k] = columnValues[sc][sortPerm[from]];
                sortMarkMax[k] = columnValues[sc][sortPerm[to - 1]];
            }
        }
        List<ByteBuf> msgs = messages;

        // build per-column, per-granule min/max + bloom + (optional) exact value set
        long[][] gMin = new long[numCols][numGranules];
        long[][] gMax = new long[numCols][numGranules];
        byte[][][] gBloom = new byte[numCols][numGranules][];
        long[][][] gSet = new long[numCols][numGranules][];
        for (int c = 0; c < numCols; c++) {
            for (int gi = 0; gi < numGranules; gi++) {
                int from = gi * g;
                int to = Math.min(from + g, n);
                long mn = Long.MAX_VALUE;
                long mx = Long.MIN_VALUE;
                List<byte[]> enc = new ArrayList<>(to - from);
                TreeSet<Long> distinct = new TreeSet<>();
                for (int i = from; i < to; i++) {
                    long v = columnValues[c][i];
                    mn = Math.min(mn, v);
                    mx = Math.max(mx, v);
                    enc.add(columnTypes[c] == TYPE_LONG ? encLong(v) : encInt((int) v));
                    distinct.add(v);
                }
                gMin[c][gi] = mn;
                gMax[c][gi] = mx;
                gBloom[c][gi] = BloomFilter.build(enc, BLOOM_BITS_PER_ELEMENT);
                if (setMaxCardinality > 0 && distinct.size() <= setMaxCardinality) {
                    long[] s = new long[distinct.size()];
                    int k = 0;
                    for (long v : distinct) {
                        s[k++] = v;
                    }
                    gSet[c][gi] = s;
                } else {
                    gSet[c][gi] = null;
                }
            }
        }

        // layout
        int dirStart = HEADER;
        int zoneMapStart = dirStart + numCols * DIR_ENTRY;
        int cursor = zoneMapStart;
        for (int c = 0; c < numCols; c++) {
            for (int gi = 0; gi < numGranules; gi++) {
                cursor += 8 + 8 + 4 + gBloom[c][gi].length + 4
                        + (gSet[c][gi] == null ? 0 : gSet[c][gi].length * 8);
            }
        }
        int sortIndexStart = cursor;
        if (hasSortIndex) {
            cursor += numGranules * 16;   // per logical granule: min(8) + max(8)
            cursor += n * 4;              // perm
        }
        // encode each column block with the smallest lossless codec (raw when compression is off)
        StreamLakeColumnCodec.Encoded[] colEnc = new StreamLakeColumnCodec.Encoded[numCols];
        boolean hasVortex = false;
        for (int c = 0; c < numCols; c++) {
            colEnc[c] = StreamLakeColumnCodec.encode(columnValues[c], n, columnTypes[c], compress);
            hasVortex |= colEnc[c].codecId != StreamLakeColumnCodec.RAW;
        }
        int[] colOffset = new int[numCols];
        for (int c = 0; c < numCols; c++) {
            colOffset[c] = cursor;
            cursor += colEnc[c].bytes.length;
        }
        int payloadIndexOffset = cursor;
        cursor += (n + 1) * 4;
        int payloadDataStart = cursor;
        int[] payloadOffsets = new int[n + 1];
        int p = payloadDataStart;
        for (int i = 0; i < n; i++) {
            payloadOffsets[i] = p;
            p += msgs.get(i).readableBytes();
        }
        payloadOffsets[n] = p;
        int total = p;

        ByteBuf out = Unpooled.buffer(total, total);
        out.writeInt(MAGIC);
        out.writeByte(VERSION);
        out.writeByte(FLAG_COLUMNAR | (hasSortIndex ? FLAG_SORTED : 0) | (hasVortex ? FLAG_VORTEX : 0));
        out.writeInt(n);
        out.writeShort(numCols);
        out.writeLong(minDate);
        out.writeLong(maxDate);
        out.writeInt(g);
        out.writeInt(numGranules);
        out.writeInt(zoneMapStart);
        out.writeInt(payloadIndexOffset);
        out.writeShort(hasSortIndex ? sortColumnId : 0);
        out.writeInt(hasSortIndex ? sortIndexStart : 0);
        // directory
        for (int c = 0; c < numCols; c++) {
            out.writeShort(columnIds[c]);
            out.writeByte(columnTypes[c]);
            out.writeByte(colEnc[c].codecId);
            out.writeInt(colOffset[c]);
            out.writeInt(colEnc[c].bytes.length);
        }
        // zone maps (column-major: all granules of col 0, then col 1, ...)
        for (int c = 0; c < numCols; c++) {
            for (int gi = 0; gi < numGranules; gi++) {
                out.writeLong(gMin[c][gi]);
                out.writeLong(gMax[c][gi]);
                out.writeInt(gBloom[c][gi].length);
                out.writeBytes(gBloom[c][gi]);
                long[] s = gSet[c][gi];
                out.writeInt(s == null ? -1 : s.length);
                if (s != null) {
                    for (long v : s) {
                        out.writeLong(v);
                    }
                }
            }
        }
        // sort index (sparse primary index over a sorted VIEW; the page itself stays in publish order)
        if (hasSortIndex) {
            for (int k = 0; k < numGranules; k++) {
                out.writeLong(sortMarkMin[k]);
                out.writeLong(sortMarkMax[k]);
            }
            for (int i = 0; i < n; i++) {
                out.writeInt(sortPerm[i]);
            }
        }
        // column data (one self-contained codec block per column, in directory order)
        for (int c = 0; c < numCols; c++) {
            out.writeBytes(colEnc[c].bytes);
        }
        // payload index + payloads
        for (int i = 0; i <= n; i++) {
            out.writeInt(payloadOffsets[i]);
        }
        for (ByteBuf m : msgs) {
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

    /** True if the page carries a sparse primary index (the page itself stays in publish order). */
    public static boolean hasSortIndex(ByteBuf page) {
        return (page.getByte(page.readerIndex() + OFF_FLAGS) & FLAG_SORTED) != 0;
    }

    /** True if any of the page's column blocks are stored with a compression codec (FLAG_VORTEX). */
    public static boolean hasCompressedColumns(ByteBuf page) {
        return (page.getByte(page.readerIndex() + OFF_FLAGS) & FLAG_VORTEX) != 0;
    }

    /** The column id the sparse primary index is built on, or 0 if none. */
    public static int sortColumnId(ByteBuf page) {
        return page.getShort(page.readerIndex() + OFF_SORT_COL) & 0xFFFF;
    }

    /** The sparse primary index: per-(sorted-)granule marks plus the row permutation. */
    public static final class SortIndex {
        /** min/max of the sort key per sorted granule (monotonic non-decreasing); bloom/set null. */
        public final GranuleStat[] marks;
        /** perm[k] = physical (publish-order) row of the k-th smallest sort-key value. */
        public final int[] perm;

        SortIndex(GranuleStat[] marks, int[] perm) {
            this.marks = marks;
            this.perm = perm;
        }
    }

    /** Decode the sparse primary index (only valid when {@link #hasSortIndex} is true). */
    public static SortIndex decodeSortIndex(ByteBuf page) {
        int base = page.readerIndex();
        int n = page.getInt(base + OFF_NUM_MESSAGES);
        int numGranules = page.getInt(base + OFF_NUM_GRANULES);
        int off = base + page.getInt(base + OFF_SORTINDEX_OFFSET);
        GranuleStat[] marks = new GranuleStat[numGranules];
        for (int k = 0; k < numGranules; k++) {
            long mn = page.getLong(off);
            long mx = page.getLong(off + 8);
            marks[k] = new GranuleStat(mn, mx, null, null);
            off += 16;
        }
        int[] perm = new int[n];
        for (int i = 0; i < n; i++) {
            perm[i] = page.getInt(off);
            off += 4;
        }
        return new SortIndex(marks, perm);
    }

    /** Decode the per-column granule zone maps: columnId -&gt; one GranuleStat per granule. */
    public static Map<Short, GranuleStat[]> decodeZoneMaps(ByteBuf page) {
        int base = page.readerIndex();
        int numCols = page.getShort(base + OFF_NUM_COLS);
        int numGranules = page.getInt(base + OFF_NUM_GRANULES);
        int zoneMapOffset = page.getInt(base + OFF_ZONEMAP_OFFSET);
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
                off += 20 + len;
                int setCount = page.getInt(off);
                off += 4;
                long[] set = null;
                if (setCount >= 0) {
                    set = new long[setCount];
                    for (int k = 0; k < setCount; k++) {
                        set[k] = page.getLong(off);
                        off += 8;
                    }
                }
                stats[gi] = new GranuleStat(mn, mx, bloom, set);
            }
            out.put(colIds[c], stats);
        }
        return out;
    }

    /** Resolved column directory entry: data origin, type, codec id, and encoded block length. */
    private static final class ColRef {
        final int origin;     // absolute byte offset of the column block in the page
        final byte type;
        final byte codecId;
        final int encodedLen;

        ColRef(int origin, byte type, byte codecId, int encodedLen) {
            this.origin = origin;
            this.type = type;
            this.codecId = codecId;
            this.encodedLen = encodedLen;
        }
    }

    private static ColRef colRef(ByteBuf page, int columnId) {
        int base = page.readerIndex();
        int numCols = page.getShort(base + OFF_NUM_COLS);
        int dir = base + HEADER;
        for (int c = 0; c < numCols; c++) {
            int entry = dir + c * DIR_ENTRY;
            if ((page.getShort(entry) & 0xFFFF) == columnId) {
                byte type = page.getByte(entry + 2);
                byte codec = page.getByte(entry + DIR_CODEC);
                int origin = base + page.getInt(entry + DIR_OFFSET);
                int encLen = page.getInt(entry + DIR_ENC_LEN);
                return new ColRef(origin, type, codec, encLen);
            }
        }
        throw new IllegalArgumentException("column not found: " + columnId);
    }

    /** Decode a whole (compressed) column block into normalized values. */
    private static long[] decodeColumnBlock(ByteBuf page, ColRef ref, int n) {
        byte[] blob = new byte[ref.encodedLen];
        page.getBytes(ref.origin, blob);
        return StreamLakeColumnCodec.decode(ref.codecId, blob, n);
    }

    /** Read column {@code columnId}'s values for rows [{@code fromRow}, {@code toRow}). */
    public static long[] readColumnRange(ByteBuf page, int columnId, int fromRow, int toRow) {
        ColRef ref = colRef(page, columnId);
        long[] values = new long[toRow - fromRow];
        if (ref.codecId == StreamLakeColumnCodec.RAW) {
            for (int i = fromRow; i < toRow; i++) {
                values[i - fromRow] = ref.type == TYPE_LONG
                        ? page.getLong(ref.origin + i * 8) : page.getInt(ref.origin + i * 4);
            }
            return values;
        }
        long[] all = decodeColumnBlock(page, ref, messageCount(page));
        System.arraycopy(all, fromRow, values, 0, toRow - fromRow);
        return values;
    }

    /** Read column {@code columnId}'s values only at the given row indices (late materialization). */
    public static long[] readColumnAt(ByteBuf page, int columnId, int[] rows, int count) {
        ColRef ref = colRef(page, columnId);
        long[] values = new long[count];
        if (ref.codecId == StreamLakeColumnCodec.RAW) {
            for (int i = 0; i < count; i++) {
                int r = rows[i];
                values[i] = ref.type == TYPE_LONG
                        ? page.getLong(ref.origin + r * 8) : page.getInt(ref.origin + r * 4);
            }
            return values;
        }
        long[] all = decodeColumnBlock(page, ref, messageCount(page));
        for (int i = 0; i < count; i++) {
            values[i] = all[rows[i]];
        }
        return values;
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
