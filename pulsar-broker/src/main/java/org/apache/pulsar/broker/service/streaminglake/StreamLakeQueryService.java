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
import org.apache.pulsar.client.streaminglake.StreamLakeBatchPayload;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
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

    private final ManagedLedger managedLedger;
    private final StreamLakeSegmentService segmentService;
    private final StreamLakePageIndex pageIndex;
    private final StreamLakeSchema schema;
    private final Executor readExecutor;
    private final int readConcurrency;
    private final StreamLakeStatistics statistics;
    private final boolean joinOffHeapEnabled;
    private final long joinMaxBuildRows;
    private final String joinSpillDir;

    private volatile StreamLakePruner pruner;
    private volatile StreamLakeQueryExecutor executor;

    private StreamLakeQueryService(ManagedLedger managedLedger, StreamLakeSegmentService segmentService,
            StreamLakePageIndex pageIndex, StreamLakeSchema schema, Executor readExecutor,
            int readConcurrency, StreamLakeStatistics statistics, boolean joinOffHeapEnabled,
            long joinMaxBuildRows, String joinSpillDir) {
        this.managedLedger = managedLedger;
        this.segmentService = segmentService;
        this.pageIndex = pageIndex;
        this.schema = schema;
        this.readExecutor = readExecutor;
        this.readConcurrency = readConcurrency;
        this.statistics = statistics;
        this.joinOffHeapEnabled = joinOffHeapEnabled;
        this.joinMaxBuildRows = joinMaxBuildRows;
        this.joinSpillDir = joinSpillDir;
    }

    public static StreamLakeQueryService create(ManagedLedger managedLedger,
            StreamLakeSegmentService segmentService, StreamLakePageIndex pageIndex, StreamLakeSchema schema,
            Executor readExecutor, int readConcurrency, StreamLakeStatistics statistics,
            boolean joinOffHeapEnabled, long joinMaxBuildRows, String joinSpillDir) {
        return new StreamLakeQueryService(managedLedger, segmentService, pageIndex, schema, readExecutor,
                readConcurrency, statistics, joinOffHeapEnabled, joinMaxBuildRows, joinSpillDir);
    }

    /**
     * A fresh hash-join build table for a join whose build side is <b>this</b> table: an off-heap
     * {@link org.apache.pulsar.client.streaminglake.SpillingJoinTable spilling} table when
     * {@code joinOffHeapEnabled} (build sides larger than heap spill row bytes to {@code joinSpillDir}),
     * else a bounded {@link org.apache.pulsar.client.streaminglake.OnHeapJoinTable on-heap} table.
     */
    public org.apache.pulsar.client.streaminglake.StreamLakeJoinTable newBuildTable() {
        return joinOffHeapEnabled
                ? new org.apache.pulsar.client.streaminglake.SpillingJoinTable(joinMaxBuildRows, joinSpillDir)
                : new org.apache.pulsar.client.streaminglake.OnHeapJoinTable(joinMaxBuildRows);
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
                            pageIndex);
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
                            readConcurrency);
                    executor = e;
                }
            }
        }
        return e;
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
