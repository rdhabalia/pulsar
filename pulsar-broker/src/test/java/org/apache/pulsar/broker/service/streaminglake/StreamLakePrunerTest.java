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
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeStatsBuilder;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Hierarchical prune (date -> segment -> page) over the catalog, segment store and page index. */
public class StreamLakePrunerTest {

    private static final String LEDGER_NAME = "tenant/ns/persistent/sl-prune";
    private static final long DAY1 = 1_700_000_000_000L;
    private static final long DAY2 = DAY1 + 24L * 3600 * 1000;

    private OrderedExecutor executor;
    private PulsarMockBookKeeper bk;
    private MetadataStore store;
    private ManagedLedger ml;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("sl-prune-test").build();
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

    private static StreamLakeSchema schema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("deptId", StreamLakeType.INT32)));
    }

    // a footer covering deptId [from, to]
    private static byte[] footer(int from, int to) {
        List<Object[]> rows = new ArrayList<>();
        for (int d = from; d <= to; d++) {
            rows.add(new Object[]{d});
        }
        return StreamLakeStatsBuilder.build(schema(), rows, Arrays.asList(0), 64, 0.01).encode();
    }

    @Test
    public void prunesByDateSegmentAndPage() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, ml, ms);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, ml, ms);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, ms);

        // Ledger 100 (DAY1): pages deptId 0-9, 10-19, 20-29, 30-39 ; Ledger 200 (DAY2): 0-9.
        for (int i = 0; i < 4; i++) {
            pageIndex.appendFooter(100L, i, footer(i * 10, i * 10 + 9));
        }
        pageIndex.appendFooter(200L, 0, footer(0, 9));
        catalog.upsert(new StreamLakeCatalog.LedgerInfo(100L, 1L, DAY1, DAY1 + 3600_000, 40,
                StreamLakeCatalog.State.CLOSED));
        catalog.upsert(new StreamLakeCatalog.LedgerInfo(200L, 1L, DAY2, DAY2 + 3600_000, 10,
                StreamLakeCatalog.State.CLOSED));

        // 2 pages per segment on ledger 100 -> segments [0-9|10-19] (entries 0-1) and [20-29|30-39] (2-3)
        StreamLakeSegmentBuilder builder = new StreamLakeSegmentBuilder(pageIndex, segStore, catalog, 2, 64, 0.01);
        builder.buildForLedger(100L);
        builder.buildForLedger(200L);

        StreamLakePruner pruner = new StreamLakePruner(catalog, segStore, pageIndex);

        // Query DAY1 only, deptId = 25 -> ledger 200 excluded by date; segment [0-19] skipped;
        // within segment [20-39], only page 20-29 (entry 2) survives.
        StreamLakeScanPredicate pred = StreamLakeScanPredicate.builder()
                .eq(0, StreamLakeType.INT32, 25).build();
        StreamLakePruner.Stats stats = new StreamLakePruner.Stats();
        List<StreamLakePruner.PagePointer> pages = pruner.prune(DAY1, DAY1 + 1000, pred, stats);

        assertEquals(pages.size(), 1, "only page deptId 20-29 survives");
        assertEquals(pages.get(0).ledgerId, 100L);
        assertEquals(pages.get(0).entryId, 2L);
        assertEquals(stats.candidateLedgers, 1, "ledger 200 pruned by date");
        assertTrue(stats.segmentsSkipped >= 1, "the deptId 0-19 segment is skipped");
        assertEquals(stats.pagesKept, 1);
    }

    @Test
    public void wideRangeKeepsAllMatchingPages() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, ml, ms);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, ml, ms);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, ms);
        for (int i = 0; i < 4; i++) {
            pageIndex.appendFooter(100L, i, footer(i * 10, i * 10 + 9));
        }
        catalog.upsert(new StreamLakeCatalog.LedgerInfo(100L, 1L, DAY1, DAY1 + 3600_000, 40,
                StreamLakeCatalog.State.CLOSED));
        new StreamLakeSegmentBuilder(pageIndex, segStore, catalog, 2, 64, 0.01).buildForLedger(100L);

        // deptId in [5, 35] -> all four pages overlap.
        StreamLakeScanPredicate pred = StreamLakeScanPredicate.builder()
                .range(0, StreamLakeType.INT32, 5, 35).build();
        List<StreamLakePruner.PagePointer> pages = pruner(catalog, segStore, pageIndex).prune(DAY1, DAY2, pred);
        assertEquals(pages.size(), 4, "all pages overlap the wide range");
    }

    private static StreamLakePruner pruner(StreamLakeCatalog c, StreamLakeSegmentStore s,
            StreamLakePageIndex p) {
        return new StreamLakePruner(c, s, p);
    }
}
