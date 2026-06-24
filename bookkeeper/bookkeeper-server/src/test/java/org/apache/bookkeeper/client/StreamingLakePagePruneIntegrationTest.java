/*
 *
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
 *
 */
package org.apache.bookkeeper.client;

import static org.junit.Assert.assertEquals;

import io.netty.buffer.Unpooled;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * End-to-end (items 1 + 6) against a real bookie: entries are written carrying an opaque
 * column-range blob via {@link LedgerHandle#asyncAddEntry(io.netty.buffer.ByteBuf, byte[],
 * AsyncCallback.AddCallback, Object)} (addEntry threads the ranges -> the bookie records
 * them into its ledger-page-index), and the broker-equivalent then asks the bookie to
 * prune via {@code BookieClient.pagePrune(...)} (PAGE_PRUNE / giveIndexPages). The bookie
 * does the pruning over its RocksDB index with no schema knowledge.
 */
public class StreamingLakePagePruneIntegrationTest extends BookKeeperClusterTestCase {

    private static final short DEPT = 2;

    public StreamingLakePagePruneIntegrationTest() {
        super(1); // single bookie so all entries + the page index live on one node
        baseConf.setLedgerStorageClass("org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage");
    }

    private static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    private static byte[] pageBlob(int deptMin, int deptMax) {
        Map<Short, PageRangeCodec.Range> cols = new HashMap<>();
        cols.put(DEPT, new PageRangeCodec.Range(encInt(deptMin), encInt(deptMax), false, false));
        return PageRangeCodec.encodePage(cols);
    }

    @Test
    public void writeEntriesWithRangesThenBookieSidePrune() throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, BookKeeper.DigestType.CRC32, "".getBytes());
        long ledgerId = lh.getId();

        // entries 0..4 -> departmentId in [1,10]; entries 5..9 -> departmentId in [11,20]
        for (int e = 0; e < 10; e++) {
            int min = e < 5 ? 1 : 11;
            int max = e < 5 ? 10 : 20;
            CompletableFuture<Long> done = new CompletableFuture<>();
            lh.asyncAddEntry(Unpooled.wrappedBuffer(("entry-" + e).getBytes()), pageBlob(min, max),
                    (rc, ledger, entryId, ctx) -> {
                        if (rc == BKException.Code.OK) {
                            done.complete(entryId);
                        } else {
                            done.completeExceptionally(BKException.create(rc));
                        }
                    }, null);
            assertEquals(Long.valueOf(e), done.get(30, TimeUnit.SECONDS));
        }
        lh.close();

        BookieId bookie = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
        BookieClient bookieClient = bkc.getClientCtx().getBookieClient();

        // predicate: departmentId > 10
        Map<Short, List<PageRangeCodec.Range>> pred = new HashMap<>();
        pred.put(DEPT, Collections.singletonList(
                new PageRangeCodec.Range(encInt(10), null, true, false)));
        byte[] predicate = PageRangeCodec.encode(pred);

        List<Long> matched =
                bookieClient.pagePrune(bookie, ledgerId, 0, 9, predicate).get(30, TimeUnit.SECONDS);

        // bookie-side pruning: entries 0-4 dropped (dept <= 10), entries 5-9 kept
        assertEquals(Arrays.asList(5L, 6L, 7L, 8L, 9L), matched);

        // a tighter predicate (departmentId > 15) keeps fewer pages
        Map<Short, List<PageRangeCodec.Range>> pred2 = new HashMap<>();
        pred2.put(DEPT, Collections.singletonList(
                new PageRangeCodec.Range(encInt(15), null, true, false)));
        List<Long> matched2 = bookieClient.pagePrune(bookie, ledgerId, 0, 9, PageRangeCodec.encode(pred2))
                .get(30, TimeUnit.SECONDS);
        // pages 5-9 hold dept [11,20], which overlaps >15, so all five remain candidates
        assertEquals(Arrays.asList(5L, 6L, 7L, 8L, 9L), matched2);
    }
}
