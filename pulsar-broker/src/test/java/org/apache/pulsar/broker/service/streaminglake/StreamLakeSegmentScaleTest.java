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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeStatsBuilder;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Scale behaviour of the redesigned metadata tier: segments are loaded on demand from their catalog
 * offset into a bounded LRU (resident memory is O(cache), not O(all data ledgers)), a data ledger's
 * page-index refs are dropped once it is segmented (resident refs stay bounded to open ledgers), and
 * every segment -- including those evicted from the cache -- still loads correctly for a query.
 */
public class StreamLakeSegmentScaleTest {

    private static final String LEDGER_NAME = "tenant/ns/persistent/sl-scale";
    private static final int DATA_LEDGERS = 8;
    private static final int PAGES_PER_LEDGER = 4;
    private static final int SEGMENT_CACHE_MAX = 2; // deliberately smaller than DATA_LEDGERS

    private OrderedExecutor executor;
    private PulsarMockBookKeeper bk;
    private MetadataStore store;
    private ManagedLedger ml;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("sl-scale-test").build();
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

    private static StreamLakeSchema schema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("id", StreamLakeType.INT32)));
    }

    private static byte[] footer(int idFrom, int idTo) {
        List<Object[]> rows = new ArrayList<>();
        for (int v = idFrom; v <= idTo; v++) {
            rows.add(new Object[]{v});
        }
        return StreamLakeStatsBuilder.build(schema(), rows, Arrays.asList(0), 64, 0.01).encode();
    }

    @Test
    public void segmentsLoadOnDemandWithBoundedResidentMemory() throws Exception {
        StreamLakeMetaStore ms = new StreamLakeMetaStore(store, ml);
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, ml, ms);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, ml, ms,
                4L * 1024 * 1024, 1, 1, 1, SEGMENT_CACHE_MAX);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, ms);
        StreamLakeSegmentBuilder builder = new StreamLakeSegmentBuilder(pageIndex, segStore, catalog,
                2L * 1024 * 1024, 0.01);

        // Build many data ledgers; page p of ledger L holds ids [L*1000 + p*10, +9].
        for (int l = 0; l < DATA_LEDGERS; l++) {
            long dataLedgerId = 1000L + l;
            for (int p = 0; p < PAGES_PER_LEDGER; p++) {
                int base = l * 1000 + p * 10;
                pageIndex.appendFooter(dataLedgerId, p, footer(base, base + 9));
            }
            catalog.upsert(new StreamLakeCatalog.LedgerInfo(dataLedgerId, 1L, 1000L + l, 1000L + l,
                    PAGES_PER_LEDGER, StreamLakeCatalog.State.CLOSED));
            builder.buildForLedger(dataLedgerId);
        }

        // Bounded resident memory: the LRU never exceeds its cap even though we built DATA_LEDGERS.
        assertTrue(segStore.cachedSegmentCount() <= SEGMENT_CACHE_MAX,
                "segment cache must stay bounded, was " + segStore.cachedSegmentCount());
        // All segmented ledgers had their page-index refs released -> no resident footer refs.
        assertEquals(pageIndex.residentRefLedgerCount(), 0,
                "segmented ledgers' page-index refs should be evicted");

        // Every ledger is segmented in the catalog with an offset.
        for (int l = 0; l < DATA_LEDGERS; l++) {
            StreamLakeCatalog.LedgerInfo info = catalog.get(1000L + l);
            assertEquals(info.state, StreamLakeCatalog.State.SEGMENTED);
            assertTrue(info.hasSegment(), "catalog carries the segment offset");
        }

        // On-demand load works for every ledger -- including ones long evicted from the LRU.
        for (int l = 0; l < DATA_LEDGERS; l++) {
            StreamLakeCatalog.LedgerInfo info = catalog.get(1000L + l);
            StreamLakeSegmentStore.LedgerSegment seg = segStore.load(info.dataLedgerId,
                    info.segmentLedgerId, info.segmentStartEntry, info.segmentEndEntry);
            assertNotNull(seg, "segment must load on demand for ledger " + info.dataLedgerId);
            assertEquals(seg.numPages(), PAGES_PER_LEDGER);
            assertEquals(seg.pageEntryIds[0], 0L);
            assertEquals(seg.pageEntryIds[PAGES_PER_LEDGER - 1], PAGES_PER_LEDGER - 1L);
        }
        assertTrue(segStore.cachedSegmentCount() <= SEGMENT_CACHE_MAX,
                "cache still bounded after on-demand loads");
    }
}
