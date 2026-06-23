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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;

/**
 * Orchestrates the prune phase of a Streaming Lake scan: stage-1 date pruning via the
 * {@link DateIndexLedger}, then a parallel stage-2 {@code PAGE_PRUNE} per candidate
 * ledger against the bookie. Returns, per ledger, the candidate page entryIds.
 *
 * <p>The runtime-specific resolutions -- which bookie in a ledger's ensemble to ask, and
 * a ledger's last entryId -- are provided as functions by the caller (the broker derives
 * them from ledger metadata). The subsequent page read + columnar decode + row filter +
 * response streaming runs against the managed ledger and is exercised under a cluster;
 * this class is the verifiable orchestration up to that boundary.
 */
public final class StreamingLakeScanExecutor {

    private StreamingLakeScanExecutor() {
    }

    /**
     * @return future of {@code ledgerId -> candidate entryIds} after date + page pruning.
     */
    public static CompletableFuture<Map<Long, List<Long>>> planPrune(
            DateIndexLedger dateIndex,
            BookieClient bookieClient,
            Function<Long, BookieId> bookieForLedger,
            Function<Long, Long> lastEntryForLedger,
            long fromMillis,
            long toMillis,
            byte[] predicate) {

        final List<Long> candidateLedgers = dateIndex.candidateLedgers(fromMillis, toMillis);
        final Map<Long, List<Long>> result = new ConcurrentHashMap<>();
        final List<CompletableFuture<Void>> perLedger = new ArrayList<>(candidateLedgers.size());

        for (final long ledgerId : candidateLedgers) {
            final BookieId bookie = bookieForLedger.apply(ledgerId);
            final long lastEntryId = lastEntryForLedger.apply(ledgerId);
            perLedger.add(bookieClient
                    .pagePrune(bookie, ledgerId, 0L, lastEntryId, predicate)
                    .thenAccept(entryIds -> {
                        if (entryIds != null && !entryIds.isEmpty()) {
                            result.put(ledgerId, entryIds);
                        }
                    }));
        }

        return CompletableFuture
                .allOf(perLedger.toArray(new CompletableFuture[0]))
                .thenApply(ignored -> result);
    }
}
