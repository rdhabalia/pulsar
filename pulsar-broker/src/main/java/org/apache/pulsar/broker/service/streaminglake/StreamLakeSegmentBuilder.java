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
import org.apache.pulsar.client.streaminglake.StreamLakeStatsMerger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compacts a closed data ledger's per-page footers into segments (redesign, async build step). Reads
 * the {@link StreamLakePageIndex} footers for a data ledger, groups every {@code pagesPerSegment}
 * consecutive pages, merges their per-column stats ({@link StreamLakeStatsMerger}) into one coarse
 * {@link StreamLakeSegmentStore.Segment}, appends it to the segment store, then marks the ledger
 * {@code SEGMENTED} in the {@link StreamLakeCatalog}. Idempotent: an already-segmented ledger is
 * skipped. Reads only metadata (footers), never the data pages.
 */
public class StreamLakeSegmentBuilder {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeSegmentBuilder.class);

    private final StreamLakePageIndex pageIndex;
    private final StreamLakeSegmentStore segmentStore;
    private final StreamLakeCatalog catalog;
    private final int pagesPerSegment;
    private final int setMaxCardinality;
    private final double bloomFpp;

    public StreamLakeSegmentBuilder(StreamLakePageIndex pageIndex, StreamLakeSegmentStore segmentStore,
            StreamLakeCatalog catalog, int pagesPerSegment, int setMaxCardinality, double bloomFpp) {
        this.pageIndex = pageIndex;
        this.segmentStore = segmentStore;
        this.catalog = catalog;
        this.pagesPerSegment = Math.max(1, pagesPerSegment);
        this.setMaxCardinality = setMaxCardinality;
        this.bloomFpp = bloomFpp;
    }

    /** Build (and durably store) the segments for one data ledger, then mark it SEGMENTED. */
    public synchronized void buildForLedger(long dataLedgerId) throws Exception {
        StreamLakeCatalog.LedgerInfo info = catalog.get(dataLedgerId);
        if (info != null && info.state == StreamLakeCatalog.State.SEGMENTED) {
            return; // idempotent
        }
        if (segmentStore.covers(dataLedgerId)) {
            catalog.markState(dataLedgerId, StreamLakeCatalog.State.SEGMENTED);
            return;
        }

        List<StreamLakePageIndex.PageFooter> footers = pageIndex.footersFor(dataLedgerId);
        for (int i = 0; i < footers.size(); i += pagesPerSegment) {
            int end = Math.min(i + pagesPerSegment, footers.size());
            List<StreamLakePageIndex.PageFooter> group = footers.subList(i, end);
            List<StreamLakeBatchStats> parts = new ArrayList<>(group.size());
            for (StreamLakePageIndex.PageFooter f : group) {
                parts.add(StreamLakeBatchStats.decode(f.stats));
            }
            StreamLakeBatchStats mergedStats = StreamLakeStatsMerger.merge(parts, setMaxCardinality, bloomFpp);
            segmentStore.appendSegment(new StreamLakeSegmentStore.Segment(
                    dataLedgerId, group.get(0).dataEntryId, group.get(group.size() - 1).dataEntryId, mergedStats));
        }

        catalog.markState(dataLedgerId, StreamLakeCatalog.State.SEGMENTED);
        log.info("StreamLake built {} segment(s) for data ledger {}",
                (footers.size() + pagesPerSegment - 1) / pagesPerSegment, dataLedgerId);
    }

    /**
     * Process the catalog's closed-but-not-yet-segmented ledgers (the segment-build work queue). This
     * is the core loop a sharded trigger consumer runs; returns the ledgers it segmented.
     */
    public synchronized List<Long> buildAllClosed() {
        List<Long> built = new ArrayList<>();
        for (long dataLedgerId : catalog.closedUnsegmented()) {
            try {
                buildForLedger(dataLedgerId);
                built.add(dataLedgerId);
            } catch (Exception e) {
                log.warn("StreamLake segment build failed for ledger {}: {}", dataLedgerId, e.toString());
            }
        }
        return built;
    }
}
