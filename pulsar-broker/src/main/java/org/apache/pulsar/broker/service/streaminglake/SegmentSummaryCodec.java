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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;

/**
 * Serialization and evaluation for {@link SegmentSummary} — the segment-level counterpart of
 * {@link PageRangeCodec} (which does the same job for a single page). One segment summary is one
 * entry in the segment index-ledger.
 *
 * <p>{@link #segmentCouldMatch} consumes exactly the predicate blob the broker already builds for
 * {@code PAGE_PRUNE} (per-column ranges plus semi-join key-sets, via
 * {@link PageRangeCodec#encodePredicate}) and mirrors {@link PageRangeCodec#pageCouldMatch}: a
 * <b>range gate</b> AND a <b>key-set gate</b> per predicate column, each sharpened to an exact test
 * when the segment carries a distinct set and falling back to min/max (conservative) otherwise. It
 * never rules out a segment that could hold a matching row.
 *
 * <pre>
 *   long  coversLedgerId
 *   long  startEntryId
 *   long  endEntryId
 *   int   numPages
 *   long  numRows
 *   short numColumns
 *   per column:
 *     short columnId
 *     byte  flags            bit0=hasMin bit1=hasMax bit2=hasSet
 *     [int minLen, bytes]    if hasMin
 *     [int maxLen, bytes]    if hasMax
 *     [int setCount, per value { int len, bytes }]   if hasSet
 * </pre>
 */
public final class SegmentSummaryCodec {

    private static final int HAS_MIN = 0x1;
    private static final int HAS_MAX = 0x2;
    private static final int HAS_SET = 0x4;

    private SegmentSummaryCodec() {
    }

    // ---------------------------------------------------------------- encode / decode

    public static byte[] encode(SegmentSummary s) {
        ByteBuffer buf = ByteBuffer.allocate(size(s));
        buf.putLong(s.coversLedgerId());
        buf.putLong(s.startEntryId());
        buf.putLong(s.endEntryId());
        buf.putInt(s.numPages());
        buf.putLong(s.numRows());
        buf.putShort((short) s.columns().size());
        for (Map.Entry<Short, SegmentSummary.ColumnSummary> e : s.columns().entrySet()) {
            SegmentSummary.ColumnSummary c = e.getValue();
            int flags = 0;
            if (c.min() != null) {
                flags |= HAS_MIN;
            }
            if (c.max() != null) {
                flags |= HAS_MAX;
            }
            if (c.set() != null) {
                flags |= HAS_SET;
            }
            buf.putShort(e.getKey());
            buf.put((byte) flags);
            if (c.min() != null) {
                buf.putInt(c.min().length);
                buf.put(c.min());
            }
            if (c.max() != null) {
                buf.putInt(c.max().length);
                buf.put(c.max());
            }
            if (c.set() != null) {
                buf.putInt(c.set().length);
                for (byte[] v : c.set()) {
                    buf.putInt(v.length);
                    buf.put(v);
                }
            }
        }
        return buf.array();
    }

    public static SegmentSummary decode(byte[] blob) {
        ByteBuffer buf = ByteBuffer.wrap(blob);
        long ledgerId = buf.getLong();
        long startEntryId = buf.getLong();
        long endEntryId = buf.getLong();
        int numPages = buf.getInt();
        long numRows = buf.getLong();
        int numCols = buf.getShort() & 0xFFFF;
        Map<Short, SegmentSummary.ColumnSummary> columns = new HashMap<>();
        for (int i = 0; i < numCols; i++) {
            short col = buf.getShort();
            int flags = buf.get() & 0xFF;
            byte[] min = null;
            byte[] max = null;
            byte[][] set = null;
            if ((flags & HAS_MIN) != 0) {
                min = new byte[buf.getInt()];
                buf.get(min);
            }
            if ((flags & HAS_MAX) != 0) {
                max = new byte[buf.getInt()];
                buf.get(max);
            }
            if ((flags & HAS_SET) != 0) {
                set = new byte[buf.getInt()][];
                for (int k = 0; k < set.length; k++) {
                    set[k] = new byte[buf.getInt()];
                    buf.get(set[k]);
                }
            }
            columns.put(col, new SegmentSummary.ColumnSummary(min, max, set));
        }
        return new SegmentSummary(ledgerId, startEntryId, endEntryId, numPages, numRows, columns);
    }

    private static int size(SegmentSummary s) {
        int size = 8 + 8 + 8 + 4 + 8 + 2; // header
        for (SegmentSummary.ColumnSummary c : s.columns().values()) {
            size += 2 + 1; // columnId + flags
            if (c.min() != null) {
                size += 4 + c.min().length;
            }
            if (c.max() != null) {
                size += 4 + c.max().length;
            }
            if (c.set() != null) {
                size += 4;
                for (byte[] v : c.set()) {
                    size += 4 + v.length;
                }
            }
        }
        return size;
    }

    // ---------------------------------------------------------------- evaluate

    /** As {@link #segmentCouldMatch(SegmentSummary, byte[])} but decoding a stored summary blob first. */
    public static boolean segmentCouldMatch(byte[] summaryBlob, byte[] predicateBlob) {
        return segmentCouldMatch(decode(summaryBlob), predicateBlob);
    }

    /**
     * True if the segment could contain a row satisfying the predicate. Per predicate column the
     * segment must pass BOTH the range gate and the key-set gate; each gate is exact when the segment
     * has a distinct set for the column and conservative (min/max, or "keep") otherwise. Schema-blind.
     */
    public static boolean segmentCouldMatch(SegmentSummary s, byte[] predicateBlob) {
        PageRangeCodec.Decoded pred = PageRangeCodec.decodeAll(predicateBlob);
        Set<Short> cols = new HashSet<>();
        cols.addAll(pred.ranges.keySet());
        cols.addAll(pred.keySets.keySet());

        for (short col : cols) {
            SegmentSummary.ColumnSummary cs = s.column(col);
            List<PageRangeCodec.Range> predRanges = pred.ranges.get(col);
            List<byte[]> keys = pred.keySets.get(col);

            // range gate
            if (predRanges != null && !predRanges.isEmpty() && cs != null) {
                if (cs.set() != null) {
                    if (!anySetValueSatisfiesAnyRange(cs.set(), predRanges)) {
                        return false;
                    }
                } else if (cs.min() != null && cs.max() != null) {
                    if (!anyRangeOverlaps(cs.min(), cs.max(), predRanges)) {
                        return false;
                    }
                }
            }

            // key-set gate (IN / semi-join probe keys)
            if (keys != null && !keys.isEmpty() && cs != null && cs.set() != null) {
                boolean anyHit = false;
                for (byte[] k : keys) {
                    if (cs.setContains(k)) {
                        anyHit = true;
                        break;
                    }
                }
                if (!anyHit) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean anySetValueSatisfiesAnyRange(byte[][] set, List<PageRangeCodec.Range> ranges) {
        for (byte[] v : set) {
            for (PageRangeCodec.Range q : ranges) {
                if (satisfiesRange(v, q)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** True if value {@code v} falls within range {@code q}, honoring open/closed bounds. */
    private static boolean satisfiesRange(byte[] v, PageRangeCodec.Range q) {
        if (q.min != null) {
            int c = PageRangeCodec.lex(v, q.min);
            if (c < 0 || (c == 0 && q.minExclusive)) {
                return false;
            }
        }
        if (q.max != null) {
            int c = PageRangeCodec.lex(v, q.max);
            if (c > 0 || (c == 0 && q.maxExclusive)) {
                return false;
            }
        }
        return true;
    }

    private static boolean anyRangeOverlaps(byte[] segMin, byte[] segMax, List<PageRangeCodec.Range> ranges) {
        for (PageRangeCodec.Range q : ranges) {
            if (rangeOverlap(segMin, segMax, q)) {
                return true;
            }
        }
        return false;
    }

    /** True if the inclusive segment interval [segMin, segMax] overlaps predicate range {@code q}. */
    private static boolean rangeOverlap(byte[] segMin, byte[] segMax, PageRangeCodec.Range q) {
        if (q.min != null) {
            int c = PageRangeCodec.lex(segMax, q.min);
            if (c < 0 || (c == 0 && q.minExclusive)) {
                return false; // segment entirely below the predicate's lower bound
            }
        }
        if (q.max != null) {
            int c = PageRangeCodec.lex(segMin, q.max);
            if (c > 0 || (c == 0 && q.maxExclusive)) {
                return false; // segment entirely above the predicate's upper bound
            }
        }
        return true;
    }
}
