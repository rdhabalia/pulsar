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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.Unpooled;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.apache.bookkeeper.bookie.storage.ldb.PageStatEntry;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

/**
 * End-to-end test for the Streaming Lake {@code PAGE_STATS} read API against a real bookie: entries
 * are written carrying an opaque column-range blob (recorded into the bookie's RocksDB page index),
 * and {@code BookieClient.pageStats(...)} then reads those raw blobs back for a ledger entry range.
 * Unlike {@code PAGE_PRUNE} (which returns filtered entryIds), this returns each page's entryId
 * paired with its verbatim blob, so an index-compaction consumer can merge them into segments.
 */
public class StreamingLakePageStatsIntegrationTest extends BookKeeperClusterTestCase {

    private static final short DEPT = 2;

    public StreamingLakePageStatsIntegrationTest() {
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

    private static PageRangeCodec.Range decodeDept(byte[] blob) {
        return PageRangeCodec.decodeAll(blob).ranges.get(DEPT).get(0);
    }

    @Test
    public void writeEntriesWithRangesThenReadPageStats() throws Exception {
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

        // full range: every page's raw blob comes back, in entryId order, decoding to its own min/max.
        List<PageStatEntry> all = bookieClient.pageStats(bookie, ledgerId, 0, 9).get(30, TimeUnit.SECONDS);
        assertEquals(10, all.size());
        for (int i = 0; i < 10; i++) {
            PageStatEntry stat = all.get(i);
            assertEquals(i, stat.getEntryId());
            int expectMin = i < 5 ? 1 : 11;
            int expectMax = i < 5 ? 10 : 20;
            assertArrayEquals("min of entry " + i, encInt(expectMin), decodeDept(stat.getBlob()).min);
            assertArrayEquals("max of entry " + i, encInt(expectMax), decodeDept(stat.getBlob()).max);
        }

        // windowing: a sub-range returns exactly those entries (bounded read for large ledgers).
        List<PageStatEntry> window = bookieClient.pageStats(bookie, ledgerId, 2, 4).get(30, TimeUnit.SECONDS);
        assertEquals(3, window.size());
        assertEquals(2, window.get(0).getEntryId());
        assertEquals(3, window.get(1).getEntryId());
        assertEquals(4, window.get(2).getEntryId());

        // out-of-range window returns nothing.
        List<PageStatEntry> none = bookieClient.pageStats(bookie, ledgerId, 100, 200).get(30, TimeUnit.SECONDS);
        assertTrue(none.isEmpty());
    }
}
