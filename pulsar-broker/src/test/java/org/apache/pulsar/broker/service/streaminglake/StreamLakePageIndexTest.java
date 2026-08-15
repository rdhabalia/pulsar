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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link StreamLakePageIndex}: footers append to a shared per-topic ledger chain,
 * read back on demand keyed by data-entry, roll the head at the size threshold, and survive a
 * reopen by replaying the chain (durability), against an in-memory {@link PulsarMockBookKeeper}.
 */
public class StreamLakePageIndexTest {

    private static final String LEDGER_NAME = "tenant/ns/persistent/sl-pageindex";

    private OrderedExecutor executor;
    private PulsarMockBookKeeper bk;
    private MetadataStore store;
    private ManagedLedger ml;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("sl-pageindex-test").build();
        bk = new PulsarMockBookKeeper(executor);
        store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());
        ml = mock(ManagedLedger.class);
        when(ml.getName()).thenReturn(LEDGER_NAME);
        when(ml.getProperties()).thenReturn(new HashMap<>());
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
        if (bk != null) {
            bk.shutdown();
            bk = null;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    private StreamLakeMetaStore metaStore() {
        return new StreamLakeMetaStore(store, ml);
    }

    private static byte[] footer(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void appendsAndReadsFootersKeyedByDataEntry() throws Exception {
        StreamLakePageIndex idx = StreamLakePageIndex.open(bk, ml, metaStore());
        idx.appendFooter(100L, 0L, footer("f100-0"));
        idx.appendFooter(100L, 1L, footer("f100-1"));
        idx.appendFooter(200L, 0L, footer("f200-0"));

        List<StreamLakePageIndex.PageFooter> l100 = idx.footersFor(100L);
        assertEquals(l100.size(), 2);
        assertEquals(l100.get(0).dataEntryId, 0L);
        assertEquals(new String(l100.get(0).stats, StandardCharsets.UTF_8), "f100-0");
        assertEquals(l100.get(1).dataEntryId, 1L);
        assertEquals(new String(l100.get(1).stats, StandardCharsets.UTF_8), "f100-1");

        assertEquals(idx.footersFor(200L).size(), 1);
        assertTrue(idx.covers(100L));
        assertFalse(idx.covers(999L));
        assertTrue(idx.footersFor(999L).isEmpty());
        idx.close();
    }

    @Test
    public void rollsHeadAtSizeThresholdIntoMultipleLedgers() throws Exception {
        // tiny threshold so each ~40-byte footer entry forces a new head ledger
        StreamLakePageIndex idx = StreamLakePageIndex.open(bk, ml, metaStore(), 30);
        for (int i = 0; i < 5; i++) {
            idx.appendFooter(100L, i, footer("footer-" + i));
        }
        List<Long> chain = metaStore().read().pageIndexLedgerIds;
        assertTrue(chain.size() > 1, "head should have rolled into multiple ledgers, got " + chain.size());

        List<StreamLakePageIndex.PageFooter> footers = idx.footersFor(100L);
        assertEquals(footers.size(), 5, "all footers readable across the rolled ledgers");
        for (int i = 0; i < 5; i++) {
            assertEquals(footers.get(i).dataEntryId, (long) i);
            assertEquals(new String(footers.get(i).stats, StandardCharsets.UTF_8), "footer-" + i);
        }
        idx.close();
    }

    @Test
    public void survivesReopenByReplayingTheChain() throws Exception {
        StreamLakePageIndex idx = StreamLakePageIndex.open(bk, ml, metaStore(), 30);
        idx.appendFooter(100L, 0L, footer("a"));
        idx.appendFooter(100L, 1L, footer("bb"));
        idx.appendFooter(300L, 0L, footer("ccc"));
        idx.close(); // like a topic unload -- an in-memory-only index would be lost here

        // reopen a fresh instance against the same bookie; it must replay the durable chain
        StreamLakePageIndex reopened = StreamLakePageIndex.open(bk, ml, metaStore());
        assertTrue(reopened.covers(100L));
        assertTrue(reopened.covers(300L));

        List<StreamLakePageIndex.PageFooter> l100 = reopened.footersFor(100L);
        assertEquals(l100.size(), 2);
        assertEquals(new String(l100.get(0).stats, StandardCharsets.UTF_8), "a");
        assertEquals(new String(l100.get(1).stats, StandardCharsets.UTF_8), "bb");
        assertEquals(new String(reopened.footersFor(300L).get(0).stats, StandardCharsets.UTF_8), "ccc");
        reopened.close();
    }

    @Test
    public void appendsAndReadsFootersAsync() throws Exception {
        StreamLakePageIndex idx = StreamLakePageIndex.open(bk, ml, metaStore());
        // Submit without waiting so the appends pipeline; a single-writer ledger acks in add order.
        java.util.List<java.util.concurrent.CompletableFuture<Void>> fs = new java.util.ArrayList<>();
        fs.add(idx.appendFooterAsync(100L, 0L, footer("f100-0")));
        fs.add(idx.appendFooterAsync(100L, 1L, footer("f100-1")));
        fs.add(idx.appendFooterAsync(200L, 0L, footer("f200-0")));
        java.util.concurrent.CompletableFuture
                .allOf(fs.toArray(new java.util.concurrent.CompletableFuture[0]))
                .get(30, java.util.concurrent.TimeUnit.SECONDS);

        List<StreamLakePageIndex.PageFooter> l100 = idx.footersFor(100L);
        assertEquals(l100.size(), 2);
        assertEquals(l100.get(0).dataEntryId, 0L);
        assertEquals(new String(l100.get(0).stats, StandardCharsets.UTF_8), "f100-0");
        assertEquals(l100.get(1).dataEntryId, 1L);
        assertEquals(new String(l100.get(1).stats, StandardCharsets.UTF_8), "f100-1");
        assertEquals(idx.footersFor(200L).size(), 1);
        assertTrue(idx.covers(100L));
        assertFalse(idx.covers(999L));
        idx.close();
    }

    @Test
    public void rollsHeadAsyncAtSizeThresholdPreservingOrder() throws Exception {
        // tiny threshold so async appends must cross the drain-barrier roll repeatedly
        StreamLakePageIndex idx = StreamLakePageIndex.open(bk, ml, metaStore(), 30);
        java.util.List<java.util.concurrent.CompletableFuture<Void>> fs = new java.util.ArrayList<>();
        for (int i = 0; i < 8; i++) {
            fs.add(idx.appendFooterAsync(100L, i, footer("footer-" + i)));
        }
        java.util.concurrent.CompletableFuture
                .allOf(fs.toArray(new java.util.concurrent.CompletableFuture[0]))
                .get(30, java.util.concurrent.TimeUnit.SECONDS);

        List<Long> chain = metaStore().read().pageIndexLedgerIds;
        assertTrue(chain.size() > 1, "async head should have rolled into multiple ledgers, got "
                + chain.size());
        List<StreamLakePageIndex.PageFooter> footers = idx.footersFor(100L);
        assertEquals(footers.size(), 8, "all async footers readable across the rolled ledgers");
        for (int i = 0; i < 8; i++) {
            assertEquals(footers.get(i).dataEntryId, (long) i, "footers stay in data-entry order");
            assertEquals(new String(footers.get(i).stats, StandardCharsets.UTF_8), "footer-" + i);
        }
        idx.close();
    }
}
