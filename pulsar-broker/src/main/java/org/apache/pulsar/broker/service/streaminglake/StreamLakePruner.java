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
        // Per-tier storage reads (for progress logging / debugging a slow query): how many segment and
        // page-index units were consulted and how many footer/segment bytes that pulled in.
        public long segmentsLoaded;
        public long segmentBytes;
        public long pageIndexReads;
        public long pageIndexBytes;
        // Candidate data ledgers skipped because they have no segment yet (still OPEN/CLOSED, not
        // SEGMENTED) while segment-sourced querying is on: the segment tier is the source of truth, so a
        // ledger with no segment yields no result rather than triggering a full page-index footer scan.
        // Non-zero here explains a query that returns slightly stale (but far cheaper) results.
        public long unsegmentedLedgersSkipped;
        // Candidate data ledgers rejected by their coarse whole-ledger summary (one tiny read) without
        // loading the full segment -- the ledger-level zone map above the per-page segment stats.
        public long ledgersSkippedBySummary;
    }

    private final StreamLakeCatalog catalog;
    private final StreamLakeSegmentStore segmentStore;
    private final StreamLakePageIndex pageIndex;
    private final boolean includeUnsegmentedLedgers;
    private final boolean useSegmentSummary;

    public StreamLakePruner(StreamLakeCatalog catalog, StreamLakeSegmentStore segmentStore,
            StreamLakePageIndex pageIndex) {
        this(catalog, segmentStore, pageIndex, true);
    }

    public StreamLakePruner(StreamLakeCatalog catalog, StreamLakeSegmentStore segmentStore,
            StreamLakePageIndex pageIndex, boolean includeUnsegmentedLedgers) {
        this(catalog, segmentStore, pageIndex, includeUnsegmentedLedgers, true);
    }

    /**
     * @param includeUnsegmentedLedgers when {@code false}, the <b>segment tier is the source of truth</b>:
     *     any data ledger that is not yet SEGMENTED (the actively-written OPEN ledger, or a just-rolled
     *     CLOSED ledger whose segment has not been built) is skipped during candidate selection, so a scan
     *     reads <b>no</b> page-index footers for it -- never bypassing the segment tier to footer-scan raw
     *     data (the query's dominant cost). The tradeoff is bounded staleness: rows only in a not-yet-
     *     segmented ledger are not visible until its segment is built. The write path is never touched;
     *     this is a read-side candidate filter only. When {@code true}, an unsegmented ledger falls back
     *     to a per-page footer prune (read-your-writes freshness, but slower).
     * @param useSegmentSummary when {@code true} (default), a segmented candidate ledger is first tested
     *     against its coarse whole-ledger summary (one small read) and skipped outright if the predicate's
     *     min/max cannot overlap it -- avoiding the full per-page segment load for ledgers that cannot
     *     match. Always conservative (never a false negative); set {@code false} to force a full segment
     *     load per candidate (debugging).
     */
    public StreamLakePruner(StreamLakeCatalog catalog, StreamLakeSegmentStore segmentStore,
            StreamLakePageIndex pageIndex, boolean includeUnsegmentedLedgers, boolean useSegmentSummary) {
        this.catalog = catalog;
        this.segmentStore = segmentStore;
        this.pageIndex = pageIndex;
        this.includeUnsegmentedLedgers = includeUnsegmentedLedgers;
        this.useSegmentSummary = useSegmentSummary;
    }

    /**
     * Sink for surviving pages, invoked once per page <b>as it is found</b> (ledger-then-entry order),
     * so a caller can stream pruning without ever buffering the full page list -- a low-selectivity scan
     * over a huge table would otherwise hold O(surviving pages) pointers in memory.
     */
    public interface PageSink {
        void accept(PagePointer page) throws Exception;
    }

    /**
     * Called once after each candidate data ledger is pruned, with the cumulative {@code stats}, so a
     * caller can log progress (which ledger / how many bytes read so far) while a slow query runs.
     */
    public interface ProgressListener {
        void afterLedger(Stats stats) throws Exception;
    }

    public List<PagePointer> prune(long fromMs, long toMs, StreamLakeScanPredicate predicate) throws Exception {
        return prune(fromMs, toMs, predicate, new Stats());
    }

    /** Prune, recording per-level counters into {@code stats}. Buffers the full surviving-page list. */
    public List<PagePointer> prune(long fromMs, long toMs, StreamLakeScanPredicate predicate, Stats stats)
            throws Exception {
        List<PagePointer> out = new ArrayList<>();
        prune(fromMs, toMs, predicate, stats, out::add);
        return out;
    }

    /**
     * Streaming prune: push each surviving page to {@code sink} as it is found (recording counters into
     * {@code stats}) without buffering the result, so peak memory is independent of the number of
     * surviving pages -- a full-table or low-selectivity scan stays bounded (only per-ledger scratch is
     * held). Pages arrive in the same ledger-then-entry order as the list-returning overloads.
     */
    public void prune(long fromMs, long toMs, StreamLakeScanPredicate predicate, Stats stats, PageSink sink)
            throws Exception {
        prune(fromMs, toMs, predicate, stats, sink, null);
    }

    /** As {@link #prune(long, long, StreamLakeScanPredicate, Stats, PageSink)} with a per-ledger progress
     * callback (nullable) for logging while a slow scan runs. */
    public void prune(long fromMs, long toMs, StreamLakeScanPredicate predicate, Stats stats, PageSink sink,
            ProgressListener progress) throws Exception {
        int[] candidateCount = {0};
        catalog.forEachCandidateLedger(fromMs, toMs, ledgerId -> {
            if (!includeUnsegmentedLedgers) {
                StreamLakeCatalog.LedgerInfo li = catalog.get(ledgerId);
                if (li == null || !li.hasSegment()) {
                    // Segment tier is the source of truth: a ledger with no segment (OPEN or just-CLOSED,
                    // not yet SEGMENTED) is not queried -- we never bypass the segment tier to footer-scan
                    // the whole ledger (the query's dominant cost). Trades bounded staleness for speed.
                    stats.unsegmentedLedgersSkipped++;
                    return;
                }
            }
            candidateCount[0]++;
            stats.candidateLedgers = candidateCount[0];
            pruneLedger(ledgerId, predicate, stats, sink);
            if (progress != null) {
                progress.afterLedger(stats);
            }
        });
        stats.candidateLedgers = candidateCount[0];
    }

    /**
     * Prune one candidate data ledger, pushing its surviving pages to {@code sink}. Split out of the
     * candidate loop so the candidate ledgers can be streamed (no {@code List<Long>} of matches held).
     */
    private void pruneLedger(long ledgerId, StreamLakeScanPredicate predicate, Stats stats, PageSink sink)
            throws Exception {
        StreamLakeCatalog.LedgerInfo info = catalog.get(ledgerId);

        // Ledger-level zone map: before loading the full per-page segment, test the coarse whole-ledger
        // summary (one tiny read). If the predicate's min/max cannot overlap it, drop the whole data
        // ledger without loading its segment. Conservative -- a missing/covering summary never excludes.
        if (useSegmentSummary && info != null && info.hasSegment()) {
            StreamLakeBatchStats summary = segmentStore.loadSummary(ledgerId, info.segmentLedgerId,
                    info.segmentStartEntry);
            if (summary != null && !predicate.matches(summary)) {
                stats.ledgersSkippedBySummary++;
                return;
            }
        }

        StreamLakeSegmentStore.LedgerSegment seg = (info != null && info.hasSegment())
                ? segmentStore.load(ledgerId, info.segmentLedgerId, info.segmentStartEntry,
                        info.segmentEndEntry)
                : null;

        if (seg == null) {
            // Not segmented yet (recent data): fall back to a per-page footer prune of this ledger.
            List<StreamLakePageIndex.PageFooter> footers = pageIndex.footersFor(ledgerId);
            stats.pageIndexReads += footers.size();
            for (StreamLakePageIndex.PageFooter f : footers) {
                stats.pageIndexBytes += f.stats.length;
                stats.pagesScanned++;
                if (predicate.matches(StreamLakeBatchStats.decode(f.stats))) {
                    stats.pagesKept++;
                    sink.accept(new PagePointer(ledgerId, f.dataEntryId));
                }
            }
            return;
        }

        // Segmented: prune to exact pages from the column segments alone (no page-footer read).
        // Per predicate column, AND together each column segment's surviving page positions.
        stats.segmentsTotal++;
        stats.segmentsLoaded++;
        stats.segmentBytes += seg.sizeBytes;
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
            List<StreamLakePageIndex.PageFooter> footers = pageIndex.readRange(info.pageIndexLedgerId,
                    info.pageIndexStartEntry, info.pageIndexEndEntry);
            stats.pageIndexReads += footers.size();
            for (StreamLakePageIndex.PageFooter f : footers) {
                stats.pageIndexBytes += f.stats.length;
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
                sink.accept(new PagePointer(ledgerId, seg.pageEntryIds[i]));
            }
        }
        if (kept == 0) {
            stats.segmentsSkipped++; // the whole segment was pruned out
        }
    }
}
