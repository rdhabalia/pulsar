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
import java.util.List;
import org.apache.pulsar.client.streaminglake.StreamLakeBatchStats;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;

/**
 * Hierarchical read-side pruning for a StreamLake scan (redesign): date -&gt; segment -&gt; page.
 * Given a time range and a {@link StreamLakeScanPredicate}, it (1) picks candidate data ledgers by
 * event-time from the {@link StreamLakeCatalog}, (2) skips whole segments whose merged stats can't
 * match ({@link StreamLakeSegmentStore}), and (3) keeps only the surviving pages by their per-page
 * footer ({@link StreamLakePageIndex}). The result is the set of data-ledger pages a scan must read;
 * surviving pages are still row-filtered on read (the pruning is conservative, never false-negative).
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
            List<StreamLakePageIndex.PageFooter> footers = pageIndex.footersFor(ledgerId);
            List<StreamLakeSegmentStore.Segment> segments = segmentStore.segmentsFor(ledgerId);

            if (segments.isEmpty()) {
                // not yet segmented: fall back to a full per-page prune of this ledger.
                for (StreamLakePageIndex.PageFooter f : footers) {
                    stats.pagesScanned++;
                    if (predicate.matches(StreamLakeBatchStats.decode(f.stats))) {
                        stats.pagesKept++;
                        out.add(new PagePointer(ledgerId, f.dataEntryId));
                    }
                }
                continue;
            }

            // segment tier: collect the entry ranges whose merged stats might match.
            List<long[]> matchedRanges = new ArrayList<>();
            for (StreamLakeSegmentStore.Segment seg : segments) {
                stats.segmentsTotal++;
                if (predicate.matches(seg.stats)) {
                    matchedRanges.add(new long[]{seg.startEntry, seg.endEntry});
                } else {
                    stats.segmentsSkipped++;
                }
            }
            if (matchedRanges.isEmpty()) {
                continue; // every segment of this ledger was skipped
            }
            // page tier: only footers inside a surviving segment range.
            for (StreamLakePageIndex.PageFooter f : footers) {
                if (!inAnyRange(f.dataEntryId, matchedRanges)) {
                    continue;
                }
                stats.pagesScanned++;
                if (predicate.matches(StreamLakeBatchStats.decode(f.stats))) {
                    stats.pagesKept++;
                    out.add(new PagePointer(ledgerId, f.dataEntryId));
                }
            }
        }
        return out;
    }

    private static boolean inAnyRange(long entryId, List<long[]> ranges) {
        for (long[] r : ranges) {
            if (entryId >= r[0] && entryId <= r[1]) {
                return true;
            }
        }
        return false;
    }
}
