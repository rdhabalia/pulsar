/*
 *
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
 *
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialization and evaluation for Streaming Lake page-range metadata.
 *
 * <p>The bookie stores, per page, an <b>opaque</b> blob of per-column min/max ranges
 * supplied by the broker. The values are order-preserving bytes, so the bookie can
 * prune pages purely by lexicographic {@code byte[]} comparison — it never decodes a
 * value, learns a column's type or name, or interprets schema.
 *
 * <p>Blob layout (used by both page blobs and predicate blobs; a page blob simply has
 * exactly one inclusive range per column):
 * <pre>
 *   short numColumns
 *   per column:
 *     short columnId
 *     short numRanges
 *     per range:
 *       byte  flags   bit0=minPresent bit1=maxPresent bit2=minExclusive bit3=maxExclusive
 *       [int minLen, bytes]   if minPresent   (absent => -infinity)
 *       [int maxLen, bytes]   if maxPresent   (absent => +infinity)
 * </pre>
 */
public final class PageRangeCodec {

    private static final int MIN_PRESENT = 0x1;
    private static final int MAX_PRESENT = 0x2;
    private static final int MIN_EXCLUSIVE = 0x4;
    private static final int MAX_EXCLUSIVE = 0x8;

    private PageRangeCodec() {
    }

    /** One column range with order-preserving byte bounds; null bound == infinity. */
    public static final class Range {
        public final byte[] min;        // null => -inf
        public final byte[] max;        // null => +inf
        public final boolean minExclusive;
        public final boolean maxExclusive;

        public Range(byte[] min, byte[] max, boolean minExclusive, boolean maxExclusive) {
            this.min = min;
            this.max = max;
            this.minExclusive = minExclusive;
            this.maxExclusive = maxExclusive;
        }

        boolean overlaps(Range q) {
            if (this.min != null && q.max != null) {
                int c = lex(this.min, q.max);
                if (c > 0 || (c == 0 && (this.minExclusive || q.maxExclusive))) {
                    return false;
                }
            }
            if (q.min != null && this.max != null) {
                int c = lex(q.min, this.max);
                if (c > 0 || (c == 0 && (q.minExclusive || this.maxExclusive))) {
                    return false;
                }
            }
            return true;
        }
    }

    /** Lexicographic unsigned byte comparison — the only comparison the bookie performs. */
    public static int lex(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int x = a[i] & 0xFF;
            int y = b[i] & 0xFF;
            if (x != y) {
                return x - y;
            }
        }
        return a.length - b.length;
    }

    // ---------------------------------------------------------------- encode

    /**
     * Optional, backward-compatible extension section appended after the range section:
     * <pre>
     *   byte  EXT_PRESENT(=1)
     *   short numBloomColumns      per: short columnId, int bloomLen, bloomBytes   (page side)
     *   short numKeySetColumns     per: short columnId, int numKeys, [int len, bytes]*  (predicate side)
     * </pre>
     * A blob without this section (a plain range blob) is read exactly as before.
     */
    private static final int EXT_PRESENT = 1;

    /** Encode a column -> list-of-ranges map (used to build both page and predicate blobs). */
    public static byte[] encode(Map<Short, List<Range>> columns) {
        ByteBuffer buf = ByteBuffer.allocate(rangeSize(columns));
        writeRanges(buf, columns);
        return buf.array();
    }

    /** Convenience: build a page blob (one inclusive range per column). */
    public static byte[] encodePage(Map<Short, Range> pageRanges) {
        return encode(toListMap(pageRanges));
    }

    /** Page blob carrying per-column min/max ranges plus a per-column value bloom filter. */
    public static byte[] encodePage(Map<Short, Range> pageRanges, Map<Short, byte[]> blooms) {
        return encodeExtended(toListMap(pageRanges), blooms, null);
    }

    /** Predicate blob carrying per-column ranges plus a per-column key-set (semi-join probe keys). */
    public static byte[] encodePredicate(Map<Short, List<Range>> ranges, Map<Short, List<byte[]>> keySets) {
        return encodeExtended(ranges, null, keySets);
    }

    private static byte[] encodeExtended(Map<Short, List<Range>> ranges,
                                         Map<Short, byte[]> blooms, Map<Short, List<byte[]>> keySets) {
        Map<Short, byte[]> bl = blooms == null ? new HashMap<>() : blooms;
        Map<Short, List<byte[]>> ks = keySets == null ? new HashMap<>() : keySets;
        int size = rangeSize(ranges) + 1 + 2 + 2;
        for (Map.Entry<Short, byte[]> e : bl.entrySet()) {
            size += 2 + 4 + e.getValue().length;
        }
        for (Map.Entry<Short, List<byte[]>> e : ks.entrySet()) {
            size += 2 + 4;
            for (byte[] k : e.getValue()) {
                size += 4 + k.length;
            }
        }
        ByteBuffer buf = ByteBuffer.allocate(size);
        writeRanges(buf, ranges);
        buf.put((byte) EXT_PRESENT);
        buf.putShort((short) bl.size());
        for (Map.Entry<Short, byte[]> e : bl.entrySet()) {
            buf.putShort(e.getKey());
            buf.putInt(e.getValue().length);
            buf.put(e.getValue());
        }
        buf.putShort((short) ks.size());
        for (Map.Entry<Short, List<byte[]>> e : ks.entrySet()) {
            buf.putShort(e.getKey());
            buf.putInt(e.getValue().size());
            for (byte[] k : e.getValue()) {
                buf.putInt(k.length);
                buf.put(k);
            }
        }
        return buf.array();
    }

    private static Map<Short, List<Range>> toListMap(Map<Short, Range> pageRanges) {
        Map<Short, List<Range>> m = new HashMap<>();
        for (Map.Entry<Short, Range> e : pageRanges.entrySet()) {
            List<Range> single = new ArrayList<>(1);
            single.add(e.getValue());
            m.put(e.getKey(), single);
        }
        return m;
    }

    private static int rangeSize(Map<Short, List<Range>> columns) {
        int size = 2;
        for (Map.Entry<Short, List<Range>> e : columns.entrySet()) {
            size += 2 + 2;
            for (Range r : e.getValue()) {
                size += 1;
                if (r.min != null) {
                    size += 4 + r.min.length;
                }
                if (r.max != null) {
                    size += 4 + r.max.length;
                }
            }
        }
        return size;
    }

    private static void writeRanges(ByteBuffer buf, Map<Short, List<Range>> columns) {
        buf.putShort((short) columns.size());
        for (Map.Entry<Short, List<Range>> e : columns.entrySet()) {
            buf.putShort(e.getKey());
            buf.putShort((short) e.getValue().size());
            for (Range r : e.getValue()) {
                int flags = 0;
                if (r.min != null) {
                    flags |= MIN_PRESENT;
                }
                if (r.max != null) {
                    flags |= MAX_PRESENT;
                }
                if (r.minExclusive) {
                    flags |= MIN_EXCLUSIVE;
                }
                if (r.maxExclusive) {
                    flags |= MAX_EXCLUSIVE;
                }
                buf.put((byte) flags);
                if (r.min != null) {
                    buf.putInt(r.min.length);
                    buf.put(r.min);
                }
                if (r.max != null) {
                    buf.putInt(r.max.length);
                    buf.put(r.max);
                }
            }
        }
    }

    // ---------------------------------------------------------------- decode

    public static Map<Short, List<Range>> decode(byte[] blob) {
        return parseRanges(ByteBuffer.wrap(blob));
    }

    /** A fully-decoded blob: per-column ranges, optional page blooms, optional predicate key-sets. */
    public static final class Decoded {
        public final Map<Short, List<Range>> ranges;
        public final Map<Short, byte[]> blooms;
        public final Map<Short, List<byte[]>> keySets;

        Decoded(Map<Short, List<Range>> ranges, Map<Short, byte[]> blooms, Map<Short, List<byte[]>> keySets) {
            this.ranges = ranges;
            this.blooms = blooms;
            this.keySets = keySets;
        }
    }

    public static Decoded decodeAll(byte[] blob) {
        ByteBuffer buf = ByteBuffer.wrap(blob);
        Map<Short, List<Range>> ranges = parseRanges(buf);
        Map<Short, byte[]> blooms = new HashMap<>();
        Map<Short, List<byte[]>> keySets = new HashMap<>();
        if (buf.hasRemaining() && (buf.get() & 0xFF) == EXT_PRESENT) {
            int nb = buf.getShort();
            for (int i = 0; i < nb; i++) {
                short col = buf.getShort();
                byte[] bloom = new byte[buf.getInt()];
                buf.get(bloom);
                blooms.put(col, bloom);
            }
            int nk = buf.getShort();
            for (int i = 0; i < nk; i++) {
                short col = buf.getShort();
                int numKeys = buf.getInt();
                List<byte[]> keys = new ArrayList<>(numKeys);
                for (int j = 0; j < numKeys; j++) {
                    byte[] k = new byte[buf.getInt()];
                    buf.get(k);
                    keys.add(k);
                }
                keySets.put(col, keys);
            }
        }
        return new Decoded(ranges, blooms, keySets);
    }

    private static Map<Short, List<Range>> parseRanges(ByteBuffer buf) {
        Map<Short, List<Range>> out = new HashMap<>();
        int numColumns = buf.getShort();
        for (int c = 0; c < numColumns; c++) {
            short columnId = buf.getShort();
            int numRanges = buf.getShort();
            List<Range> ranges = new ArrayList<>(numRanges);
            for (int r = 0; r < numRanges; r++) {
                int flags = buf.get() & 0xFF;
                byte[] min = null;
                byte[] max = null;
                if ((flags & MIN_PRESENT) != 0) {
                    min = new byte[buf.getInt()];
                    buf.get(min);
                }
                if ((flags & MAX_PRESENT) != 0) {
                    max = new byte[buf.getInt()];
                    buf.get(max);
                }
                ranges.add(new Range(min, max,
                        (flags & MIN_EXCLUSIVE) != 0, (flags & MAX_EXCLUSIVE) != 0));
            }
            out.put(columnId, ranges);
        }
        return out;
    }

    // ---------------------------------------------------------------- evaluate

    /**
     * True if a page could contain a row satisfying the predicate. For each predicate column the
     * page must pass BOTH the range test (AND across columns, OR within a column's ranges) and the
     * key-set/bloom test (the page's value bloom must possibly-contain at least one probe key).
     * Conservative: a page lacking a range or a bloom for a column is kept. Schema-agnostic.
     */
    public static boolean pageCouldMatch(byte[] pageBlob, byte[] predicateBlob) {
        Decoded page = decodeAll(pageBlob);
        Decoded pred = decodeAll(predicateBlob);

        java.util.Set<Short> cols = new java.util.HashSet<>();
        cols.addAll(pred.ranges.keySet());
        cols.addAll(pred.keySets.keySet());

        for (short col : cols) {
            // range test
            List<Range> predRanges = pred.ranges.get(col);
            if (predRanges != null && !predRanges.isEmpty()) {
                List<Range> pageRanges = page.ranges.get(col);
                if (pageRanges != null && !pageRanges.isEmpty()) {
                    Range pageRange = pageRanges.get(0);
                    boolean anyOverlap = false;
                    for (Range q : predRanges) {
                        if (pageRange.overlaps(q)) {
                            anyOverlap = true;
                            break;
                        }
                    }
                    if (!anyOverlap) {
                        return false;
                    }
                }
            }
            // key-set / bloom test
            List<byte[]> keys = pred.keySets.get(col);
            if (keys != null && !keys.isEmpty()) {
                byte[] bloom = page.blooms.get(col);
                if (bloom != null) {
                    boolean anyHit = false;
                    for (byte[] k : keys) {
                        if (BloomFilter.mightContain(bloom, k)) {
                            anyHit = true;
                            break;
                        }
                    }
                    if (!anyHit) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}
