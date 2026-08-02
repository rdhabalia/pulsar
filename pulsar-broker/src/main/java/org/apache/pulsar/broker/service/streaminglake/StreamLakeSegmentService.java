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

import java.util.List;
import java.util.concurrent.Executor;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker-hosted read-side builder for a StreamLake topic. The write path only slices each batch's
 * stats footer into the shared page-index ledger; that alone is <b>not queryable</b>, because
 * {@link StreamLakePruner} starts from the {@link StreamLakeCatalog}'s candidate ledgers -- an empty
 * catalog yields no candidates and a scan returns nothing even though the footers are durable. This
 * service closes that gap: as data ledgers roll over it (1) registers each ledger's event-time bounds
 * in the catalog so date pruning can reach it, and (2) rolls the ledger's already-durable page-index
 * footers into column {@link StreamLakeSegmentStore segments} so pruning lands on the exact surviving
 * pages.
 *
 * <p><b>Trigger.</b> Ledger close is detected on the topic's ordered executor from the data-ledger id
 * changing between consecutive persisted entries (a new id means the previous ledger rolled and all
 * its pages are durable). Detection + the two per-ledger catalog upserts are cheap and stay on that
 * thread; the heavier segment build (read N footers, write a segment ledger) is dispatched to
 * {@code buildExecutor} so it never sits on the producer ack path.
 *
 * <p><b>Event time.</b> A ledger's {@code [minEventTime, maxEventTime]} is the broker <i>ingest</i>
 * time (persist wall-clock) of its first/last entry. For an append-in-order live stream this is a
 * correct, monotonic date-prune key; a future enhancement can instead derive the bounds from a
 * designated time column's footer stats for backfill/event-time correctness.
 *
 * <p><b>Crash safety.</b> The {@code CLOSED} catalog upsert is durable before the build is dispatched,
 * so a crash between close and build is healed on the next open by draining
 * {@link StreamLakeSegmentBuilder#buildAllClosed()}. A ledger that closes at the exact moment of a
 * crash (no following entry to trigger detection) is reconciled by that same drain once its state is
 * recorded; full ledger-list reconciliation on open is a documented follow-up.
 */
public final class StreamLakeSegmentService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeSegmentService.class);

    private final String topicName;
    private final StreamLakeCatalog catalog;
    private final StreamLakeSegmentStore segmentStore;
    private final StreamLakeSegmentBuilder builder;
    private final Executor buildExecutor;

    // Mutated only on the topic ordered executor (single-threaded per topic); guarded for visibility.
    private long currentLedgerId = -1;
    private long minEventTime = Long.MAX_VALUE;
    private long maxEventTime = Long.MIN_VALUE;
    private long rowCount;

    private StreamLakeSegmentService(String topicName, StreamLakeCatalog catalog,
            StreamLakeSegmentStore segmentStore, StreamLakeSegmentBuilder builder, Executor buildExecutor) {
        this.topicName = topicName;
        this.catalog = catalog;
        this.segmentStore = segmentStore;
        this.builder = builder;
        this.buildExecutor = buildExecutor;
    }

    /**
     * Open the catalog + segment store for a topic, wire a segment builder, and asynchronously drain
     * any closed-but-unsegmented ledgers left by a previous run (crash recovery). Never throws.
     */
    public static StreamLakeSegmentService open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
            StreamLakePageIndex pageIndex, StreamingLakeConfig cfg, Executor buildExecutor) {
        StreamLakeSegmentStore segmentStore = StreamLakeSegmentStore.open(bk, ml, metaStore,
                1L << 30, cfg.getSegmentMaxEntriesPerLedger(), cfg.getSegmentEnsembleSize(),
                cfg.getSegmentWriteQuorum(), cfg.getSegmentAckQuorum(), cfg.getSegmentCacheMaxEntries());
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, metaStore);
        StreamLakeSegmentBuilder builder = new StreamLakeSegmentBuilder(pageIndex, segmentStore, catalog,
                cfg.getSegmentColumnMaxBytes(), cfg.getBloomFpp());
        StreamLakeSegmentService service = new StreamLakeSegmentService(ml.getName(), catalog, segmentStore,
                builder, buildExecutor);
        buildExecutor.execute(() -> {
            try {
                // Recover any ledger that closed but never got segmented last run.
                List<Long> built = builder.buildAllClosed();
                if (!built.isEmpty()) {
                    log.info("StreamLake recovered {} unsegmented ledger(s) for {}", built.size(), ml.getName());
                }
                // Reopen replays every footer into the page index; drop the resident refs for ledgers
                // that are already segmented (reachable via their catalog offset) so resident memory
                // stays bounded to open/unsegmented ledgers.
                for (StreamLakeCatalog.LedgerInfo info : catalog.all().values()) {
                    if (info.hasSegment()) {
                        pageIndex.releaseRefs(info.dataLedgerId);
                    }
                }
            } catch (RuntimeException e) {
                log.warn("StreamLake segment recovery failed for {}: {}", ml.getName(), e.toString());
            }
        });
        return service;
    }

    /**
     * Record a just-persisted data entry (called on the topic ordered executor, after its footer has
     * been appended). Tracks the current ledger's event-time bounds; on the first entry of a ledger it
     * registers the ledger {@code OPEN} (so recent data is immediately queryable via the pruner's
     * per-page fallback), and when the data-ledger id advances it finalizes the previous ledger
     * ({@code CLOSED} + dispatch a segment build).
     *
     * @param ledgerId    the data ledger the entry landed in
     * @param entryId     the data entry id (unused today; kept for future per-entry accounting)
     * @param eventTimeMs the entry's ingest (persist) time
     * @param numMessages messages in the batch (row-count accounting)
     */
    public synchronized void onEntryPersisted(long ledgerId, long entryId, long eventTimeMs, long numMessages) {
        if (currentLedgerId != -1 && ledgerId != currentLedgerId) {
            finalizeLedger(currentLedgerId, minEventTime, maxEventTime, rowCount);
            resetTracking();
        }
        if (currentLedgerId != ledgerId) {
            // First entry of a new (open) ledger: register it so date pruning can already reach it.
            currentLedgerId = ledgerId;
            registerOpen(ledgerId, eventTimeMs);
        }
        minEventTime = Math.min(minEventTime, eventTimeMs);
        maxEventTime = Math.max(maxEventTime, eventTimeMs);
        rowCount += Math.max(1, numMessages);
    }

    private void registerOpen(long ledgerId, long eventTimeMs) {
        try {
            // maxEventTime = MAX_VALUE while OPEN keeps the still-growing ledger a candidate for any
            // window up to now; it is rewritten with the true bound when the ledger closes.
            catalog.upsert(new StreamLakeCatalog.LedgerInfo(ledgerId, System.currentTimeMillis(),
                    eventTimeMs, Long.MAX_VALUE, 0, StreamLakeCatalog.State.OPEN));
        } catch (RuntimeException e) {
            log.warn("StreamLake open-ledger register failed for {} ledger {}: {}",
                    topicName, ledgerId, e.toString());
        }
    }

    private void finalizeLedger(long ledgerId, long minEt, long maxEt, long rows) {
        try {
            // Durably record CLOSED + true event-time bounds BEFORE the (async) build, so a crash in
            // between is healed by buildAllClosed() on the next open.
            catalog.upsert(new StreamLakeCatalog.LedgerInfo(ledgerId, System.currentTimeMillis(),
                    minEt, maxEt, rows, StreamLakeCatalog.State.CLOSED));
        } catch (RuntimeException e) {
            log.warn("StreamLake close-ledger register failed for {} ledger {}: {}",
                    topicName, ledgerId, e.toString());
            return;
        }
        buildExecutor.execute(() -> {
            try {
                builder.buildForLedger(ledgerId);
            } catch (Exception e) {
                log.warn("StreamLake segment build failed for {} ledger {}: {}",
                        topicName, ledgerId, e.toString());
            }
        });
    }

    private void resetTracking() {
        minEventTime = Long.MAX_VALUE;
        maxEventTime = Long.MIN_VALUE;
        rowCount = 0;
    }

    public StreamLakeCatalog catalog() {
        return catalog;
    }

    public StreamLakeSegmentStore segmentStore() {
        return segmentStore;
    }

    public StreamLakeSegmentBuilder builder() {
        return builder;
    }

    @Override
    public void close() {
        try {
            catalog.close();
        } catch (RuntimeException e) {
            log.warn("StreamLake catalog close failed for {}: {}", topicName, e.toString());
        }
    }
}
