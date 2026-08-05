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
import java.util.Map;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.client.streaminglake.StreamLakeArrowBatchEncoder;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeStatsBuilder;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.apache.pulsar.common.policies.data.StreamLakeQueryStats;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** End-to-end query execution: prune -> read pages -> exact row filter, plus top-K, over a mock reader. */
public class StreamLakeQueryExecutorTest {

    private static final String LEDGER_NAME = "tenant/ns/persistent/sl-query";
    private static final long DATA_LEDGER = 100L;
    private static final long DAY1 = 1_700_000_000_000L;

    private OrderedExecutor executor;
    private PulsarMockBookKeeper bk;
    private MetadataStore store;
    private ManagedLedger ml;

    // the page reader: entryId -> raw Arrow batch bytes
    private final Map<Long, byte[]> pageBytes = new HashMap<>();

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("sl-query-test").build();
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
                new StreamLakeSchema.Column("id", StreamLakeType.INT32),
                new StreamLakeSchema.Column("deptId", StreamLakeType.INT32),
                new StreamLakeSchema.Column("salary", StreamLakeType.INT64)));
    }

    // encode a page's rows to Arrow (stored for the reader) and append its footer to the page index.
    private void addPage(StreamLakePageIndex pageIndex, long entryId, List<Object[]> rows) throws Exception {
        try (StreamLakeArrowBatchEncoder enc = new StreamLakeArrowBatchEncoder(schema())) {
            pageBytes.put(entryId, enc.encode(rows));
        }
        byte[] footer = StreamLakeStatsBuilder.build(schema(), rows, Arrays.asList(1, 2), 64, 0.01).encode();
        pageIndex.appendFooter(DATA_LEDGER, entryId, footer);
    }

    @Test
    public void scanPrunesThenExactlyFilters() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, ml, ms);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, ml, ms);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, ms);

        addPage(pageIndex, 0, Arrays.asList(new Object[]{1, 1, 100L}, new Object[]{2, 1, 400L}));
        addPage(pageIndex, 1, java.util.Collections.singletonList(new Object[]{3, 2, 500L}));
        addPage(pageIndex, 2, java.util.Collections.singletonList(new Object[]{4, 1, 900L}));
        addPage(pageIndex, 3, java.util.Collections.singletonList(new Object[]{5, 3, 50L}));
        catalog.upsert(new StreamLakeCatalog.LedgerInfo(DATA_LEDGER, 1L, DAY1, DAY1 + 3600_000, 5,
                StreamLakeCatalog.State.CLOSED));
        new StreamLakeSegmentBuilder(pageIndex, segStore, catalog, 2L * 1024 * 1024, 0.01).buildForLedger(DATA_LEDGER);

        StreamLakePruner pruner = new StreamLakePruner(catalog, segStore, pageIndex);
        StreamLakeQueryExecutor.PageReader reader = (lid, eid) -> pageBytes.get(eid);
        StreamLakeQueryExecutor engine = new StreamLakeQueryExecutor(pruner, reader);

        // WHERE deptId = 1 AND salary >= 300
        StreamLakeScanPredicate pred = StreamLakeScanPredicate.builder()
                .eq(1, StreamLakeType.INT32, 1)
                .range(2, StreamLakeType.INT64, 300L, null)
                .build();
        List<Object[]> rows = engine.scan(DAY1, DAY1 + 1000, pred);

        // dept-1 salaries >= 300 are id=2 (400) and id=4 (900); dept-2/3 pages pruned; id=1 (100) filtered.
        assertEquals(rows.size(), 2);
        List<Long> salaries = new ArrayList<>();
        for (Object[] r : rows) {
            assertEquals(r[1], 1, "only deptId 1 rows survive");
            salaries.add((Long) r[2]);
        }
        assertTrue(salaries.contains(400L) && salaries.contains(900L));

        // ORDER BY salary DESC LIMIT 1
        List<Object[]> top = engine.scanTopK(DAY1, DAY1 + 1000, pred, 1, 2, true);
        assertEquals(top.size(), 1);
        assertEquals(top.get(0)[2], 900L);
    }

    @Test
    public void scanAccumulatesQueryMetrics() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, ml, ms);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, ml, ms);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, ms);

        addPage(pageIndex, 0, Arrays.asList(new Object[]{1, 1, 100L}, new Object[]{2, 1, 400L}));
        addPage(pageIndex, 1, java.util.Collections.singletonList(new Object[]{3, 2, 500L}));
        addPage(pageIndex, 2, java.util.Collections.singletonList(new Object[]{4, 1, 900L}));
        addPage(pageIndex, 3, java.util.Collections.singletonList(new Object[]{5, 3, 50L}));
        catalog.upsert(new StreamLakeCatalog.LedgerInfo(DATA_LEDGER, 1L, DAY1, DAY1 + 3600_000, 5,
                StreamLakeCatalog.State.CLOSED));
        new StreamLakeSegmentBuilder(pageIndex, segStore, catalog, 2L * 1024 * 1024, 0.01)
                .buildForLedger(DATA_LEDGER);

        StreamLakePruner pruner = new StreamLakePruner(catalog, segStore, pageIndex);
        StreamLakeQueryExecutor.PageReader reader = (lid, eid) -> pageBytes.get(eid);
        StreamLakeQueryMetrics metrics = new StreamLakeQueryMetrics();
        StreamLakeQueryExecutor engine = new StreamLakeQueryExecutor(pruner, reader, null, 1, metrics);

        // WHERE deptId = 1 -> the dept-2 and dept-3 pages prune; both dept-1 pages are read (3 rows).
        StreamLakeScanPredicate pred = StreamLakeScanPredicate.builder()
                .eq(1, StreamLakeType.INT32, 1).build();
        assertEquals(engine.scan(DAY1, DAY1 + 1000, pred).size(), 3);

        StreamLakeQueryStats s = metrics.toStats();
        assertEquals(s.getPagesScanned(), 4, "all 4 segment pages checked");
        assertEquals(s.getPagesKept(), 2, "only the 2 dept-1 pages survive");
        assertEquals(s.getPagesPruned(), 2, "the 2 non-dept-1 pages pruned");
        assertEquals(s.getRowsRead(), 3, "rows decoded from the 2 kept pages");
        assertTrue(s.getBytesRead() > 0, "read the kept pages' Arrow bytes");
        assertTrue(s.getPeakReadBufferBytes() > 0, "a bounded read buffer was used");
    }

    @Test
    public void parallelReadsMatchSerial() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, ml, ms);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, ml, ms);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, ms);

        // Many pages so a read-ahead window (concurrency 4) actually overlaps reads.
        int pages = 20;
        for (int p = 0; p < pages; p++) {
            List<Object[]> rows = new ArrayList<>();
            for (int r = 0; r < 5; r++) {
                int id = p * 5 + r;
                rows.add(new Object[]{id, id % 3, 100L + id});
            }
            addPage(pageIndex, p, rows);
        }
        catalog.upsert(new StreamLakeCatalog.LedgerInfo(DATA_LEDGER, 1L, DAY1, DAY1 + 3600_000, pages * 5,
                StreamLakeCatalog.State.CLOSED));
        new StreamLakeSegmentBuilder(pageIndex, segStore, catalog, 2L * 1024 * 1024, 0.01)
                .buildForLedger(DATA_LEDGER);

        StreamLakePruner pruner = new StreamLakePruner(catalog, segStore, pageIndex);
        // A reader that sleeps a touch so serial vs parallel timing differs but results must not.
        StreamLakeQueryExecutor.PageReader reader = (lid, eid) -> {
            try {
                Thread.sleep(2);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            return pageBytes.get(eid);
        };
        StreamLakeScanPredicate pred = StreamLakeScanPredicate.builder()
                .eq(1, StreamLakeType.INT32, 1).build();

        StreamLakeQueryExecutor serial = new StreamLakeQueryExecutor(pruner, reader);
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(4);
        try {
            StreamLakeQueryExecutor parallel = new StreamLakeQueryExecutor(pruner, reader, pool, 4);
            List<Object[]> a = serial.scan(DAY1, DAY1 + 1000, pred);
            List<Object[]> b = parallel.scan(DAY1, DAY1 + 1000, pred);
            assertEquals(b.size(), a.size(), "parallel scan returns the same row count as serial");
            // ids appear in page order in both; compare the id column element-wise.
            for (int i = 0; i < a.size(); i++) {
                assertEquals(b.get(i)[0], a.get(i)[0], "row " + i + " id must match (page order preserved)");
                assertEquals(b.get(i)[1], 1, "only deptId 1 rows survive");
            }
            assertTrue(a.size() > 4, "predicate should keep enough rows to span the read-ahead window");
        } finally {
            pool.shutdownNow();
        }
    }
}
