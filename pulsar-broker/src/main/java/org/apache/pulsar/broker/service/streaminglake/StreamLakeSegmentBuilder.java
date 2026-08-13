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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.streaminglake.StreamLakeBatchStats;
import org.apache.pulsar.client.streaminglake.StreamLakeColumnSegment;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compacts a closed data ledger's per-page footers into a column-oriented segment (async build step).
 * Reads the {@link StreamLakePageIndex} footers for the ledger, builds a page directory (position -&gt;
 * data entryId) and one {@link StreamLakeColumnSegment} per indexed column (each holding that column's
 * per-page stats, collapsing to a whole-segment stat past {@code segmentColumnMaxBytes}), appends them
 * to the {@link StreamLakeSegmentStore}, then marks the ledger {@code SEGMENTED} in the
 * {@link StreamLakeCatalog}. Idempotent: an already-segmented ledger is skipped. Reads only metadata
 * (footers), never the data pages.
 */
public class StreamLakeSegmentBuilder {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeSegmentBuilder.class);

    private final StreamLakePageIndex pageIndex;
    private final StreamLakeSegmentStore segmentStore;
    private final StreamLakeCatalog catalog;
    private final long segmentColumnMaxBytes;
    private final double bloomFpp;

    public StreamLakeSegmentBuilder(StreamLakePageIndex pageIndex, StreamLakeSegmentStore segmentStore,
            StreamLakeCatalog catalog, long segmentColumnMaxBytes, double bloomFpp) {
        this.pageIndex = pageIndex;
        this.segmentStore = segmentStore;
        this.catalog = catalog;
        this.segmentColumnMaxBytes = segmentColumnMaxBytes > 0 ? segmentColumnMaxBytes : 2L * 1024 * 1024;
        this.bloomFpp = bloomFpp;
    }

    /** Build (and durably store) the column-oriented segment for one data ledger, then mark SEGMENTED. */
    public synchronized void buildForLedger(long dataLedgerId) throws Exception {
        buildForLedger(dataLedgerId, 0);
    }

    /**
     * As {@link #buildForLedger(long)}, recording the data ledger's on-storage {@code sizeBytes} (supplied
     * by the owning broker that closed it) plus the exact record count summed from the per-page footers,
     * into the catalog -- so a table's total size and records are a plain catalog scan on any broker.
     */
    public synchronized void buildForLedger(long dataLedgerId, long sizeBytes) throws Exception {
        StreamLakeCatalog.LedgerInfo info = catalog.get(dataLedgerId);
        if (info != null && info.state == StreamLakeCatalog.State.SEGMENTED) {
            return; // idempotent
        }

        List<StreamLakePageIndex.PageFooter> footers = pageIndex.footersFor(dataLedgerId);
        if (footers.isEmpty()) {
            catalog.markState(dataLedgerId, StreamLakeCatalog.State.SEGMENTED);
            pageIndex.releaseRefs(dataLedgerId);
            return;
        }

        int numPages = footers.size();
        long[] pageEntryIds = new long[numPages];
        List<StreamLakeBatchStats> perPage = new ArrayList<>(numPages);
        long recordCount = 0;
        // first-seen column order + type (columns are the union of what the pages index).
        Map<Integer, StreamLakeType> columnTypes = new LinkedHashMap<>();
        for (int i = 0; i < numPages; i++) {
            pageEntryIds[i] = footers.get(i).dataEntryId;
            StreamLakeBatchStats stats = StreamLakeBatchStats.decode(footers.get(i).stats);
            perPage.add(stats);
            recordCount += stats.rowCount(); // exact rows, summed from the per-page footers
            for (StreamLakeBatchStats.ColumnStats cs : stats.columns()) {
                columnTypes.putIfAbsent(cs.columnIndex(), cs.type());
            }
        }

        List<StreamLakeColumnSegment> columns = new ArrayList<>(columnTypes.size());
        for (Map.Entry<Integer, StreamLakeType> e : columnTypes.entrySet()) {
            int col = e.getKey();
            List<StreamLakeBatchStats.ColumnStats> perPageCol = new ArrayList<>(numPages);
            for (StreamLakeBatchStats stats : perPage) {
                perPageCol.add(stats.column(col)); // null for a page that didn't index this column
            }
            columns.add(StreamLakeColumnSegment.build(col, e.getValue(), perPageCol,
                    segmentColumnMaxBytes, bloomFpp));
        }

        // Capture the data ledger's page-index range (for on-demand exact-set precision) before it is
        // evicted, then record the segment offset + page-index range + size/records in the catalog manifest.
        long[] piRange = pageIndex.getFooterRange(dataLedgerId);
        long[] segOffset = segmentStore.appendLedgerSegment(dataLedgerId, pageEntryIds, columns);
        catalog.markSegmented(dataLedgerId, segOffset[0], segOffset[1], segOffset[2],
                piRange[0], piRange[1], piRange[2], sizeBytes, recordCount);
        pageIndex.releaseRefs(dataLedgerId);
        log.info("StreamLake built segment for data ledger {} ({} pages, {} records, {} bytes, {} columns)",
                dataLedgerId, numPages, recordCount, sizeBytes, columns.size());
    }

    /**
     * Process the catalog's closed-but-not-yet-segmented ledgers (the segment-build work queue). This
     * is the core loop a sharded trigger consumer runs; returns the ledgers it segmented.
     */
    public synchronized List<Long> buildAllClosed() {
        return buildAllClosed(id -> 0);
    }

    public synchronized List<Long> buildAllClosed(java.util.function.LongUnaryOperator sizeProvider) {
        List<Long> built = new ArrayList<>();
        for (long dataLedgerId : catalog.closedUnsegmented()) {
            try {
                buildForLedger(dataLedgerId, sizeProvider.applyAsLong(dataLedgerId));
                built.add(dataLedgerId);
            } catch (Exception e) {
                log.warn("StreamLake segment build failed for ledger {}: {}", dataLedgerId, e.toString());
            }
        }
        return built;
    }
}
