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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.client.streaminglake.OnHeapJoinTable;
import org.apache.pulsar.client.streaminglake.SpillingJoinTable;
import org.apache.pulsar.client.streaminglake.StreamLakeBatchPayload;
import org.apache.pulsar.client.streaminglake.StreamLakeJoinTable;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeTopicSchema;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.apache.pulsar.common.protocol.Commands;

/**
 * The broker-hosted, per-topic query service: it owns the seam between StreamLake's read-side metadata
 * (catalog + segment store + page index) and the topic's data pages, and hands the caller a ready
 * {@link StreamLakeQueryExecutor}. This is where query orchestration lives on the broker (not in the
 * client, and not hand-assembled by callers): a {@link StreamLakeQueryExecutor.PageReader} reads a
 * pruned page from <i>this</i> topic's managed ledger and strips the message framing to the Arrow
 * bytes, and the executor is built with the topic's configured read concurrency for parallel prefetch.
 *
 * <p>The {@link StreamLakeQueryCoordinator} composes one service per table to run single-table scans
 * and inner joins; a client only ever submits a query (SQL) and receives rows.
 */
public final class StreamLakeQueryService {

    /** The build-table backend for a broadcast join. */
    public enum Backend { ONHEAP, SPILL, ROCKSDB }

    private final ManagedLedger managedLedger;
    private final StreamLakeSegmentService segmentService;
    private final StreamLakePageIndex pageIndex;
    private final StreamingLakeConfig cfg;
    private final Executor readExecutor;
    private final StreamLakeSchema schema;
    private final StreamLakeStatistics statistics;
    private final String brokerJoinSpillDir;
    private final boolean brokerQueryIncludeUnsegmentedLedgers;

    private volatile StreamLakePruner pruner;
    private volatile StreamLakeQueryExecutor executor;

    private StreamLakeQueryService(ManagedLedger managedLedger, StreamLakeSegmentService segmentService,
            StreamLakePageIndex pageIndex, StreamingLakeConfig cfg, Executor readExecutor,
            String brokerJoinSpillDir, boolean brokerQueryIncludeUnsegmentedLedgers) {
        this.managedLedger = managedLedger;
        this.segmentService = segmentService;
        this.pageIndex = pageIndex;
        this.cfg = cfg;
        this.readExecutor = readExecutor;
        this.schema = StreamLakeTopicSchema.fromConfig(cfg).schema();
        this.statistics = new StreamLakeStatistics(cfg.getEstimatedRowsPerPage(), cfg.getEstimatedPageBytes());
        this.brokerJoinSpillDir = brokerJoinSpillDir == null ? "" : brokerJoinSpillDir;
        this.brokerQueryIncludeUnsegmentedLedgers = brokerQueryIncludeUnsegmentedLedgers;
    }

    public static StreamLakeQueryService create(ManagedLedger managedLedger,
            StreamLakeSegmentService segmentService, StreamLakePageIndex pageIndex, StreamingLakeConfig cfg,
            Executor readExecutor, String brokerJoinSpillDir,
            boolean brokerQueryIncludeUnsegmentedLedgers) {
        return new StreamLakeQueryService(managedLedger, segmentService, pageIndex, cfg, readExecutor,
                brokerJoinSpillDir, brokerQueryIncludeUnsegmentedLedgers);
    }

    /** The topic's StreamLake config (join strategy, budgets, RocksDB sizes, ...). */
    public StreamingLakeConfig config() {
        return cfg;
    }

    public StreamingLakeConfig.JoinStrategy joinStrategy() {
        return cfg.getJoinStrategy();
    }

    public boolean joinLargeBuildUsesRocksDb() {
        return cfg.isJoinLargeBuildUsesRocksDb();
    }

    /** Max on-disk partitions for the Grace join (caps ceil(buildBytes/budget)). */
    public int joinMaxPartitions() {
        return cfg.getJoinMaxPartitions();
    }

    /** Build-memory budget (bytes): broadcast if the build side fits this, else a large-build operator. */
    public long joinBuildMemoryBudget() {
        return cfg.getJoinBuildMemoryBudget();
    }

    /** Per-partition / build-side row admission guard. */
    public long joinMaxBuildRows() {
        return cfg.getJoinMaxBuildRows();
    }

    /**
     * Directory for join spill / partition / RocksDB files. The broker-wide
     * {@code streamLakeJoinSpillDir} takes precedence (join spill is a server-side storage concern); a
     * per-topic {@code joinSpillDir} is an optional override. Empty on both means the JVM temp dir.
     */
    public String joinSpillDir() {
        if (brokerJoinSpillDir != null && !brokerJoinSpillDir.isEmpty()) {
            return brokerJoinSpillDir;
        }
        return cfg.getJoinSpillDir();
    }

    /** Estimated result-row guard (0 = disabled). */
    public long runawayResultRows() {
        return cfg.getRunawayResultRows();
    }

    /** Off-heap RocksDB block-cache cap for out-of-core operators. */
    public long rocksdbBlockCacheBytes() {
        return cfg.getRocksdbBlockCacheBytes();
    }

    /** Off-heap RocksDB write-buffer size for out-of-core operators. */
    public long rocksdbWriteBufferBytes() {
        return cfg.getRocksdbWriteBufferBytes();
    }

    /**
     * A fresh broadcast-join build table with the requested backend: {@code ONHEAP} (fast, bounded),
     * {@code SPILL} (row bytes to a file, on-heap key index), or {@code ROCKSDB} (keys+values on disk,
     * bounded off-heap) — for build sides far larger than the JVM heap.
     */
    public StreamLakeJoinTable newBuildTable(Backend backend) {
        switch (backend) {
            case ROCKSDB:
                return new RocksDbJoinTable(joinSpillDir(), cfg.getRocksdbBlockCacheBytes(),
                        cfg.getRocksdbWriteBufferBytes());
            case SPILL:
                return new SpillingJoinTable(cfg.getJoinMaxBuildRows(), joinSpillDir());
            default:
                return new OnHeapJoinTable(cfg.getJoinMaxBuildRows());
        }
    }

    /** The default build table for the broadcast path: spilling when configured, else on-heap. */
    public StreamLakeJoinTable newBuildTable() {
        return newBuildTable(cfg.isJoinOffHeapEnabled() ? Backend.SPILL : Backend.ONHEAP);
    }

    /** The table's columnar schema (from its StreamLake topic policy). */
    public StreamLakeSchema schema() {
        return schema;
    }

    /** The pruner over this topic's catalog + segments + page index (shared by the executor + stats). */
    public StreamLakePruner pruner() {
        StreamLakePruner p = pruner;
        if (p == null) {
            synchronized (this) {
                p = pruner;
                if (p == null) {
                    p = new StreamLakePruner(segmentService.catalog(), segmentService.segmentStore(),
                            pageIndex, brokerQueryIncludeUnsegmentedLedgers);
                    pruner = p;
                }
            }
        }
        return p;
    }

    /**
     * Metadata-only estimate of how much data {@code predicate} touches after pruning (for the planner's
     * join-strategy selection). No data pages are read.
     */
    public StreamLakeStatistics.Estimate estimate(long fromMs, long toMs,
            org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate predicate) throws Exception {
        return statistics.estimate(pruner(), fromMs, toMs, predicate);
    }

    /** A ready executor over this topic (prune -> parallel page read -> exact filter / join). */
    public StreamLakeQueryExecutor executor() {
        StreamLakeQueryExecutor e = executor;
        if (e == null) {
            synchronized (this) {
                e = executor;
                if (e == null) {
                    e = new StreamLakeQueryExecutor(pruner(), this::readArrowBatch, readExecutor,
                            cfg.getQueryReadConcurrency());
                    executor = e;
                }
            }
        }
        return e;
    }

    /**
     * A fresh per-query executor bound to {@code metrics} (not cached, so concurrent queries don't share
     * counters). Both sides of a join pass the same metrics instance so the totals are whole-query.
     */
    public StreamLakeQueryExecutor executor(StreamLakeQueryMetrics metrics) {
        return new StreamLakeQueryExecutor(pruner(), this::readArrowBatch, readExecutor,
                cfg.getQueryReadConcurrency(), metrics);
    }

    // Read one pruned page (a data-ledger entry) from the managed ledger and strip the message
    // metadata + payload framing down to the Arrow batch bytes.
    private byte[] readArrowBatch(long ledgerId, long entryId) throws Exception {
        CompletableFuture<byte[]> f = new CompletableFuture<>();
        Position pos = PositionFactory.create(ledgerId, entryId);
        managedLedger.asyncReadEntry(pos, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    ByteBuf buf = entry.getDataBuffer();
                    Commands.parseMessageMetadata(buf); // advance past the message metadata
                    byte[] payload = new byte[buf.readableBytes()];
                    buf.getBytes(buf.readerIndex(), payload);
                    f.complete(StreamLakeBatchPayload.arrowBatch(payload));
                } catch (Throwable t) {
                    f.completeExceptionally(t);
                } finally {
                    entry.release();
                }
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                f.completeExceptionally(exception);
            }
        }, null);
        return f.get(60, TimeUnit.SECONDS);
    }
}
