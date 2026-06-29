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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;

/**
 * Broker-side batcher for a StreamLake topic. Buffers published messages and, once the page
 * reaches the target size, message count, or grouping window, packs them into one columnar
 * page entry ({@link StreamLakeBatchPage}), ships per-column min/max ranges to the bookie, and
 * completes every buffered publish only after the page is persisted (correct durability).
 *
 * <p>Producer-receipt note: all N messages in a page complete with the same (ledgerId, entryId)
 * because {@link PublishContext#completed} carries no batch index; the consumer side recovers
 * distinct batch indices when the page is transcoded back into a standard batch on read.
 */
public class StreamLakeBatcher {

    private final ManagedLedger ledger;
    private final StreamingLakeConfig config;
    private final ScheduledExecutorService scheduler;
    private final int maxBytes;
    private final int maxMessages;
    private final long maxDelayMs;

    private final List<ByteBuf> messages = new ArrayList<>();
    private final List<PublishContext> contexts = new ArrayList<>();
    private int bufferedBytes;
    private ScheduledFuture<?> flushTask;
    // point 7: durable per-ledger date-partition index for ledger-level pruning.
    private final StreamLakeDateIndex dateIndex;

    /** Per-ledger {minEventTime, maxEventTime} (durable, replayed from the date-partition ledger). */
    public java.util.Map<Long, long[]> getLedgerDateRanges() {
        return dateIndex.ranges();
    }

    public StreamLakeBatcher(ManagedLedger ledger, StreamingLakeConfig config,
                             ScheduledExecutorService scheduler, StreamLakeDateIndex dateIndex) {
        this.ledger = ledger;
        this.config = config;
        this.scheduler = scheduler;
        this.dateIndex = dateIndex;
        this.maxBytes = config.getPageSizeBytes() > 0 ? config.getPageSizeBytes() : 2 * 1024 * 1024;
        this.maxMessages = config.getMaxPageMessages() > 0 ? config.getMaxPageMessages() : 1000;
        this.maxDelayMs = config.getPageGroupingDelayMs() > 0 ? config.getPageGroupingDelayMs() : 10;
    }

    public synchronized void add(ByteBuf headersAndPayload, PublishContext ctx) {
        messages.add(headersAndPayload.retainedDuplicate());
        contexts.add(ctx);
        bufferedBytes += headersAndPayload.readableBytes();
        if (messages.size() == 1) {
            flushTask = scheduler.schedule(this::flush, maxDelayMs, TimeUnit.MILLISECONDS);
        }
        if (bufferedBytes >= maxBytes || messages.size() >= maxMessages) {
            sealLocked();
        }
    }

    public synchronized void flush() {
        sealLocked();
    }

    private void sealLocked() {
        if (messages.isEmpty()) {
            return;
        }
        if (flushTask != null) {
            flushTask.cancel(false);
            flushTask = null;
        }
        final List<ByteBuf> batch = new ArrayList<>(messages);
        final List<PublishContext> ctxs = new ArrayList<>(contexts);
        messages.clear();
        contexts.clear();
        bufferedBytes = 0;

        final StreamLakeRangeBuilder.ColumnData cols = StreamLakeRangeBuilder.extractColumns(config, batch);
        final long[] dateRange = StreamLakeRangeBuilder.dateRange(batch);
        final ByteBuf page = StreamLakeBatchPage.encode(batch, cols.columnIds, cols.columnTypes, cols.values,
                dateRange[0], dateRange[1], config.getGranuleSize());
        final byte[] ranges = StreamLakeRangeBuilder.buildForBatch(config, batch);
        for (ByteBuf b : batch) {
            b.release();
        }

        ledger.asyncAddEntry(page, batch.size(), ranges, new AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                // point 7: durably record the ledger's date range for ledger-level pruning.
                dateIndex.record(position.getLedgerId(), dateRange[0], dateRange[1]);
                for (PublishContext pc : ctxs) {
                    pc.completed(null, position.getLedgerId(), position.getEntryId());
                }
                page.release();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                for (PublishContext pc : ctxs) {
                    pc.completed(exception, -1, -1);
                }
                page.release();
            }
        }, null);
    }

    /** Flush and drop anything buffered (topic close). */
    public synchronized void close() {
        sealLocked();
    }
}
