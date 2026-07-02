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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;

/**
 * A <b>segment summary</b>: the merged L0 page statistics for a contiguous run of pages within one
 * ledger (design: the segment-level metadata index that sits between the date-partition prune and
 * the bookie {@code PAGE_PRUNE}). It is to a <b>segment</b> what a {@link PageRangeCodec} blob is to
 * a single <b>page</b>, one level up: per-column <b>min/max</b> (the envelope of the pages' ranges)
 * and, for low-cardinality columns, an exact <b>distinct set</b> (the union of the pages' sets).
 *
 * <p>The summary is built by streaming a ledger's page stats through {@link Builder} once (no full
 * materialization) and is evaluated against the same predicate blob the broker already builds for
 * {@code PAGE_PRUNE} (see {@link SegmentSummaryCodec#segmentCouldMatch}). Merges are monotone, so a
 * segment the summary rules out cannot contain any page that would itself pass — pruning a segment
 * only ever skips work, never a matching row.
 *
 * <p>Exact-set correctness rule: a segment's distinct set is kept <b>only</b> if every contributing
 * page supplied its own exact set and the running union stayed within the cap. If any page carried
 * values for a column but no exact set (its per-page cardinality exceeded the cap), the segment set
 * for that column is dropped to {@code null} and pruning falls back to min/max.
 */
public final class SegmentSummary {

    private final long coversLedgerId;
    private final long startEntryId; // first page entryId in this segment (inclusive)
    private final long endEntryId;   // last page entryId in this segment (inclusive)
    private final int numPages;
    private final long numRows;       // best-effort row count (0 if unknown)
    private final Map<Short, ColumnSummary> columns;

    SegmentSummary(long coversLedgerId, long startEntryId, long endEntryId, int numPages, long numRows,
                   Map<Short, ColumnSummary> columns) {
        this.coversLedgerId = coversLedgerId;
        this.startEntryId = startEntryId;
        this.endEntryId = endEntryId;
        this.numPages = numPages;
        this.numRows = numRows;
        this.columns = columns;
    }

    public long coversLedgerId() {
        return coversLedgerId;
    }

    public long startEntryId() {
        return startEntryId;
    }

    public long endEntryId() {
        return endEntryId;
    }

    public int numPages() {
        return numPages;
    }

    public long numRows() {
        return numRows;
    }

    public Map<Short, ColumnSummary> columns() {
        return columns;
    }

    public ColumnSummary column(short columnId) {
        return columns.get(columnId);
    }

    /** Per-column merged statistics: inclusive min/max bounds and an optional exact distinct set. */
    public static final class ColumnSummary {
        private final byte[] min;    // inclusive lower bound (order-preserving bytes); null => unknown
        private final byte[] max;    // inclusive upper bound; null => unknown
        private final byte[][] set;  // sorted distinct values, or null if not an exact set

        public ColumnSummary(byte[] min, byte[] max, byte[][] set) {
            this.min = min;
            this.max = max;
            this.set = set;
        }

        public byte[] min() {
            return min;
        }

        public byte[] max() {
            return max;
        }

        public byte[][] set() {
            return set;
        }

        /** Exact membership test over the sorted distinct set; only valid when {@link #set} != null. */
        public boolean setContains(byte[] value) {
            int lo = 0;
            int hi = set.length - 1;
            while (lo <= hi) {
                int mid = (lo + hi) >>> 1;
                int c = PageRangeCodec.lex(set[mid], value);
                if (c < 0) {
                    lo = mid + 1;
                } else if (c > 0) {
                    hi = mid - 1;
                } else {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Streaming builder that folds one page's stats at a time into a running segment summary. Feed it
     * either decoded per-column ranges + optional sets, or a raw L0 page blob.
     */
    public static final class Builder {

        private final long coversLedgerId;
        private final int setCap;
        private long startEntryId = -1;
        private long endEntryId = -1;
        private int numPages;
        private long numRows;
        private final Map<Short, ColState> cols = new HashMap<>();

        private static final class ColState {
            byte[] min;
            byte[] max;
            TreeSet<byte[]> set = new TreeSet<>(PageRangeCodec::lex);
            boolean setValid = true; // false once a page had values but no exact set, or the union exceeded the cap
        }

        /**
         * @param coversLedgerId the ledger whose pages this segment summarizes
         * @param setCap         keep a column's exact distinct set only while its size stays within this
         */
        public Builder(long coversLedgerId, int setCap) {
            this.coversLedgerId = coversLedgerId;
            this.setCap = setCap;
        }

        /**
         * Fold one page in.
         *
         * @param entryId     the page's bookie entryId
         * @param rowsInPage  the page's row count (best-effort; pass 0 if unknown)
         * @param ranges      per-column inclusive min/max for this page (from its L0 blob)
         * @param sets        per-column sorted distinct values when the page carried an exact set,
         *                    else no entry for that column (the segment set then drops to min/max)
         */
        public Builder addPage(long entryId, long rowsInPage, Map<Short, PageRangeCodec.Range> ranges,
                               Map<Short, byte[][]> sets) {
            if (startEntryId < 0 || entryId < startEntryId) {
                startEntryId = entryId;
            }
            if (entryId > endEntryId) {
                endEntryId = entryId;
            }
            numPages++;
            numRows += Math.max(0, rowsInPage);
            for (Map.Entry<Short, PageRangeCodec.Range> e : ranges.entrySet()) {
                short col = e.getKey();
                PageRangeCodec.Range r = e.getValue();
                ColState st = cols.computeIfAbsent(col, k -> new ColState());
                if (r.min != null && (st.min == null || PageRangeCodec.lex(r.min, st.min) < 0)) {
                    st.min = r.min;
                }
                if (r.max != null && (st.max == null || PageRangeCodec.lex(r.max, st.max) > 0)) {
                    st.max = r.max;
                }
                byte[][] pageSet = sets == null ? null : sets.get(col);
                if (!st.setValid) {
                    continue;
                }
                if (pageSet == null) {
                    // the page had values for this column (it has a range) but no exact set -> not exact.
                    st.setValid = false;
                    st.set = null;
                } else {
                    for (byte[] v : pageSet) {
                        st.set.add(v);
                    }
                    if (st.set.size() > setCap) {
                        st.setValid = false;
                        st.set = null;
                    }
                }
            }
            return this;
        }

        /**
         * Fold one page in from its raw L0 blob: min/max from the range section, and the per-page
         * exact set from the set section when present (a column with a range but no set contributes
         * values without an exact set, so the segment set for that column is dropped).
         */
        public Builder addPage(long entryId, long rowsInPage, byte[] l0Blob) {
            PageRangeCodec.Decoded d = PageRangeCodec.decodeAll(l0Blob);
            Map<Short, PageRangeCodec.Range> ranges = new HashMap<>();
            for (Map.Entry<Short, List<PageRangeCodec.Range>> e : d.ranges.entrySet()) {
                if (!e.getValue().isEmpty()) {
                    ranges.put(e.getKey(), e.getValue().get(0));
                }
            }
            Map<Short, byte[][]> sets = null;
            if (d.sets != null && !d.sets.isEmpty()) {
                sets = new HashMap<>();
                for (Map.Entry<Short, List<byte[]>> e : d.sets.entrySet()) {
                    sets.put(e.getKey(), e.getValue().toArray(new byte[0][]));
                }
            }
            return addPage(entryId, rowsInPage, ranges, sets);
        }

        public SegmentSummary build() {
            Map<Short, ColumnSummary> out = new HashMap<>();
            for (Map.Entry<Short, ColState> e : cols.entrySet()) {
                ColState st = e.getValue();
                byte[][] set = st.setValid && st.set != null ? st.set.toArray(new byte[0][]) : null;
                out.put(e.getKey(), new ColumnSummary(st.min, st.max, set));
            }
            return new SegmentSummary(coversLedgerId, startEntryId < 0 ? 0 : startEntryId,
                    endEntryId < 0 ? 0 : endEntryId, numPages, numRows, out);
        }
    }
}
