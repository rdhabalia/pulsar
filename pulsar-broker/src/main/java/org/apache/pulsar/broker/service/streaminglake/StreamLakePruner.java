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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.streaminglake.StreamLakeBatchStats;
import org.apache.pulsar.client.streaminglake.StreamLakeColumnSegment;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;

/**
 * Hierarchical read-side pruning for a StreamLake scan: date -&gt; page. Given a time range and a
 * {@link StreamLakeScanPredicate}, it (1) picks candidate data ledgers by event-time from the
 * {@link StreamLakeCatalog}, then for each ledger (2a) if it is <b>segmented</b>, prunes straight to
 * the exact candidate pages from the column-oriented {@link StreamLakeSegmentStore} (ANDing each
 * predicate column's surviving page positions -- no per-page footer read), or (2b) if it is not yet
 * segmented (recent data), falls back to a per-page prune of the {@link StreamLakePageIndex} footers.
 * The result is the set of data-ledger pages a scan must read; surviving pages are still row-filtered
 * on read (the pruning is conservative, never false-negative).
 */
public class StreamLakePruner {

    /** A page (one data-ledger entry) that survived pruning and must be read. */
    public static final class PagePointer {
        public final long ledgerId;
        public final long entryId;

        public PagePointer(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }
    }

    /** Per-scan pruning counters (observability: how aggressively each level cut work). */
    public static final class Stats {
        public int candidateLedgers;
        public int segmentsTotal;
        public int segmentsSkipped;
        public int pagesScanned;
        public int pagesKept;
    }

    private final StreamLakeCatalog catalog;
    private final StreamLakeSegmentStore segmentStore;
    private final StreamLakePageIndex pageIndex;

    public StreamLakePruner(StreamLakeCatalog catalog, StreamLakeSegmentStore segmentStore,
            StreamLakePageIndex pageIndex) {
        this.catalog = catalog;
        this.segmentStore = segmentStore;
        this.pageIndex = pageIndex;
    }

    public List<PagePointer> prune(long fromMs, long toMs, StreamLakeScanPredicate predicate) throws Exception {
        return prune(fromMs, toMs, predicate, new Stats());
    }

    /** Prune, recording per-level counters into {@code stats}. */
    public List<PagePointer> prune(long fromMs, long toMs, StreamLakeScanPredicate predicate, Stats stats)
            throws Exception {
        List<PagePointer> out = new ArrayList<>();
        List<Long> candidates = catalog.candidateLedgers(fromMs, toMs);
        stats.candidateLedgers = candidates.size();

        for (long ledgerId : candidates) {
            StreamLakeCatalog.LedgerInfo info = catalog.get(ledgerId);
            StreamLakeSegmentStore.LedgerSegment seg = (info != null && info.hasSegment())
                    ? segmentStore.load(ledgerId, info.segmentLedgerId, info.segmentStartEntry,
                            info.segmentEndEntry)
                    : null;

            if (seg == null) {
                // Not segmented yet (recent data): fall back to a per-page footer prune of this ledger.
                for (StreamLakePageIndex.PageFooter f : pageIndex.footersFor(ledgerId)) {
                    stats.pagesScanned++;
                    if (predicate.matches(StreamLakeBatchStats.decode(f.stats))) {
                        stats.pagesKept++;
                        out.add(new PagePointer(ledgerId, f.dataEntryId));
                    }
                }
                continue;
            }

            // Segmented: prune to exact pages from the column segments alone (no page-footer read).
            // Per predicate column, AND together each column segment's surviving page positions.
            stats.segmentsTotal++;
            int numPages = seg.numPages();
            boolean[] surviving = new boolean[numPages];
            Arrays.fill(surviving, true);
            boolean anyCollapsed = false;
            for (StreamLakeScanPredicate.ColumnPredicate cp : predicate.columns()) {
                StreamLakeColumnSegment cseg = seg.columns.get(cp.columnIndex());
                if (cseg == null) {
                    continue; // this column has no segment stats -> cannot prune on it
                }
                anyCollapsed |= cseg.collapsed();
                boolean[] col = cseg.candidatePositions(cp);
                for (int i = 0; i < numPages; i++) {
                    surviving[i] &= col[i];
                }
            }
            // Precision recheck: when a predicate column collapsed (its per-page granularity was lost to
            // a whole-segment stat), consult the data ledger's exact page-index range and drop pages the
            // exact footer (incl. set(N)) rejects. Always safe -- the footer test is never a false negative.
            if (anyCollapsed && info.pageIndexLedgerId >= 0) {
                Map<Long, byte[]> statsByEntry = new HashMap<>();
                for (StreamLakePageIndex.PageFooter f : pageIndex.readRange(info.pageIndexLedgerId,
                        info.pageIndexStartEntry, info.pageIndexEndEntry)) {
                    statsByEntry.put(f.dataEntryId, f.stats);
                }
                for (int i = 0; i < numPages; i++) {
                    if (surviving[i]) {
                        byte[] fs = statsByEntry.get(seg.pageEntryIds[i]);
                        if (fs != null && !predicate.matches(StreamLakeBatchStats.decode(fs))) {
                            surviving[i] = false;
                        }
                    }
                }
            }
            int kept = 0;
            for (int i = 0; i < numPages; i++) {
                stats.pagesScanned++;
                if (surviving[i]) {
                    stats.pagesKept++;
                    kept++;
                    out.add(new PagePointer(ledgerId, seg.pageEntryIds[i]));
                }
            }
            if (kept == 0) {
                stats.segmentsSkipped++; // the whole segment was pruned out
            }
        }
        return out;
    }
}
