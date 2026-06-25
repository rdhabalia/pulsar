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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.protocol.Commands;

/**
 * StreamLake predicate scan over a live batched topic (design points 10-13): push the column
 * predicate to each ledger's bookie via {@code PAGE_PRUNE}, then read only the surviving page
 * entries, decode them, and keep just the rows that match the exact predicate.
 *
 * <p>(Date-partition pruning -- point 9 -- would prepend a date-ledger filter; that index is not
 * yet maintained on the live topic, so this scans all of the topic's ledgers.)
 */
public final class StreamLakePageScan {

    private StreamLakePageScan() {
    }

    /** An INT-column predicate: exclusive lower ({@code gt}) and/or upper ({@code lt}) bound. */
    public static final class Bound {
        public final short columnId;
        public final String columnName;
        public final Integer gt;
        public final Integer lt;

        public Bound(short columnId, String columnName, Integer gt, Integer lt) {
            this.columnId = columnId;
            this.columnName = columnName;
            this.gt = gt;
            this.lt = lt;
        }
    }

    /** Scan result: matching row payloads plus how many ledgers were scanned vs date-pruned. */
    public static final class ScanResult {
        public final List<byte[]> rows;
        public final int ledgersScanned;
        public final int ledgersPrunedByDate;

        ScanResult(List<byte[]> rows, int ledgersScanned, int ledgersPrunedByDate) {
            this.rows = rows;
            this.ledgersScanned = ledgersScanned;
            this.ledgersPrunedByDate = ledgersPrunedByDate;
        }
    }

    /** @return the value payloads of every row across the topic matching all bounds. */
    public static List<byte[]> scan(PersistentTopic topic, BookKeeper bk, List<Bound> bounds)
            throws Exception {
        return scan(topic, bk, bounds, Long.MIN_VALUE, Long.MAX_VALUE).rows;
    }

    /**
     * Three-level prune: (9) skip ledgers whose date range is outside [{@code fromDate},
     * {@code toDate}], (10) bookie PAGE_PRUNE on column ranges, (12) selective row decode.
     */
    public static ScanResult scan(PersistentTopic topic, BookKeeper bk, List<Bound> bounds,
                                  long fromDate, long toDate) throws Exception {
        ManagedLedger ml = topic.getManagedLedger();
        byte[] predicate = buildPredicate(bounds);
        BookieClient bookieClient = bk.getClientCtx().getBookieClient();
        // The current (still-open) ledger reports 0 entries in its LedgerInfo; use the LAC for it.
        Position lac = ml.getLastConfirmedEntry();
        Map<Long, long[]> dateIndex = topic.getStreamLakeDateIndex();
        boolean dateFilter = fromDate != Long.MIN_VALUE || toDate != Long.MAX_VALUE;

        List<byte[]> result = new ArrayList<>();
        int scanned = 0;
        int prunedByDate = 0;
        for (Map.Entry<Long, LedgerInfo> e : ml.getLedgersInfo().entrySet()) {
            long ledgerId = e.getKey();
            long lastEntry = (lac != null && lac.getLedgerId() == ledgerId)
                    ? lac.getEntryId() : e.getValue().getEntries() - 1;
            if (lastEntry < 0) {
                continue;
            }
            // point 9: date-partition prune -- skip ledgers entirely outside the query window
            if (dateFilter) {
                long[] dr = dateIndex.get(ledgerId);
                if (dr != null && (dr[1] < fromDate || dr[0] > toDate)) {
                    prunedByDate++;
                    continue;
                }
            }
            scanned++;
            BookieId bookie = bk.getLedgerManager().readLedgerMetadata(ledgerId)
                    .get(30, TimeUnit.SECONDS).getValue().getAllEnsembles().firstEntry().getValue().get(0);
            // point 10/11: bookie prunes pages by column range
            List<Long> pages = bookieClient
                    .pagePrune(bookie, ledgerId, 0, lastEntry, predicate)
                    .get(30, TimeUnit.SECONDS);
            for (long entryId : pages) {
                // point 12/13: read only surviving pages, evaluate the predicate on the columns,
                // then decode ONLY the matching rows' payloads (selective decode).
                ByteBuf pageData = readEntry(topic, ledgerId, entryId);
                try {
                    int n = StreamLakeBatchPage.messageCount(pageData);
                    boolean[] keep = new boolean[n];
                    java.util.Arrays.fill(keep, true);
                    for (Bound b : bounds) {
                        long[] values = StreamLakeBatchPage.readColumn(pageData, b.columnId);
                        for (int i = 0; i < n; i++) {
                            if (b.gt != null && !(values[i] > b.gt)) {
                                keep[i] = false;
                            }
                            if (b.lt != null && !(values[i] < b.lt)) {
                                keep[i] = false;
                            }
                        }
                    }
                    for (int i = 0; i < n; i++) {
                        if (!keep[i]) {
                            continue;
                        }
                        ByteBuf msg = StreamLakeBatchPage.messageAt(pageData, i);
                        try {
                            result.add(extractPayload(msg));
                        } finally {
                            msg.release();
                        }
                    }
                } finally {
                    pageData.release();
                }
            }
        }
        return new ScanResult(result, scanned, prunedByDate);
    }

    private static byte[] extractPayload(ByteBuf msg) {
        Commands.parseMessageMetadata(msg); // advances msg past magic/checksum/metadata to the payload
        byte[] payload = new byte[msg.readableBytes()];
        msg.getBytes(msg.readerIndex(), payload);
        return payload;
    }

    private static byte[] buildPredicate(List<Bound> bounds) {
        Map<Short, List<PageRangeCodec.Range>> pred = new HashMap<>();
        for (Bound b : bounds) {
            byte[] min = b.gt != null ? encInt(b.gt) : null;
            byte[] max = b.lt != null ? encInt(b.lt) : null;
            pred.computeIfAbsent(b.columnId, k -> new ArrayList<>())
                    .add(new PageRangeCodec.Range(min, max, b.gt != null, b.lt != null));
        }
        return PageRangeCodec.encode(pred);
    }

    private static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    private static ByteBuf readEntry(PersistentTopic topic, long ledgerId, long entryId)
            throws Exception {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();
        topic.asyncReadEntry(PositionFactory.create(ledgerId, entryId), new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                ByteBuf copy = entry.getDataBuffer().retainedDuplicate();
                entry.release();
                future.complete(copy);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future.get(30, TimeUnit.SECONDS);
    }

    /** Convenience for a single departmentId-style greater-than predicate. */
    public static List<Bound> gt(short columnId, String columnName, int value) {
        return Collections.singletonList(new Bound(columnId, columnName, value, null));
    }
}
