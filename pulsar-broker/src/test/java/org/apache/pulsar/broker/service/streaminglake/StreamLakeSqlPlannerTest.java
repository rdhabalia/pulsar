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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.client.streaminglake.StreamLakeArrowBatchEncoder;
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
 * Parses SQL with the Calcite frontend ({@link StreamLakeSqlPlanner}) and runs it end to end through
 * {@link StreamLakeQueryExecutor#executeSql}, verifying pushdown pruning, exact bound strictness,
 * ORDER BY / LIMIT top-K, projection, and the event-time window translation.
 */
public class StreamLakeSqlPlannerTest {

    private static final String LEDGER_NAME = "tenant/ns/persistent/sl-sql";
    private static final long DATA_LEDGER = 100L;
    private static final long DAY1 = 1_700_000_000_000L;

    private OrderedExecutor executor;
    private PulsarMockBookKeeper bk;
    private MetadataStore store;
    private ManagedLedger ml;
    private StreamLakeQueryExecutor cachedEngine;

    private final Map<Long, byte[]> pageBytes = new HashMap<>();

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("sl-sql-test").build();
        bk = new PulsarMockBookKeeper(executor);
        store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());
        ml = mock(ManagedLedger.class);
        when(ml.getName()).thenReturn(LEDGER_NAME);
        when(ml.getProperties()).thenReturn(new HashMap<>());
        cachedEngine = null;
        pageBytes.clear();
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
                new StreamLakeSchema.Column("id", StreamLakeType.INT32),
                new StreamLakeSchema.Column("deptId", StreamLakeType.INT32),
                new StreamLakeSchema.Column("salary", StreamLakeType.INT64)));
    }

    private void addPage(StreamLakePageIndex pageIndex, long entryId, List<Object[]> rows) throws Exception {
        try (StreamLakeArrowBatchEncoder enc = new StreamLakeArrowBatchEncoder(schema())) {
            pageBytes.put(entryId, enc.encode(rows));
        }
        byte[] footer = StreamLakeStatsBuilder.build(schema(), rows, Arrays.asList(1, 2), 64, 0.01).encode();
        pageIndex.appendFooter(DATA_LEDGER, entryId, footer);
    }

    private StreamLakeQueryExecutor engine() throws Exception {
        // Build the storage stack + pages once per test; re-opening would re-append footers.
        if (cachedEngine != null) {
            return cachedEngine;
        }
        StreamLakeMetaStore ms = new StreamLakeMetaStore(store, ml);
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
        cachedEngine = new StreamLakeQueryExecutor(pruner, (lid, eid) -> pageBytes.get(eid));
        return cachedEngine;
    }

    @Test
    public void parsesSelectProjectionOrderByLimit() {
        StreamLakeSqlPlanner.Plan plan = StreamLakeSqlPlanner.plan(
                "SELECT id, salary FROM employee WHERE deptId = 1 AND salary >= 300 "
                        + "ORDER BY salary DESC LIMIT 1", schema());
        assertEquals(plan.table(), "employee");
        assertEquals(plan.projection(), new int[]{0, 2});
        assertEquals(plan.sortColumn(), 2);
        assertTrue(plan.descending());
        assertEquals(plan.limit(), 1);
        assertEquals(plan.fromMs(), Long.MIN_VALUE);
        assertEquals(plan.toMs(), Long.MAX_VALUE);
    }

    @Test
    public void starProjectionIsNull() {
        StreamLakeSqlPlanner.Plan plan = StreamLakeSqlPlanner.plan(
                "SELECT * FROM employee WHERE deptId = 1", schema());
        assertNull(plan.projection());
        assertEquals(plan.sortColumn(), -1);
        assertEquals(plan.limit(), -1);
    }

    @Test
    public void timePredicatesBecomeWindowNotRowFilter() {
        StreamLakeSqlPlanner.Plan plan = StreamLakeSqlPlanner.plan(
                "SELECT id FROM employee WHERE ts >= 100 AND ts < 200 AND deptId = 1", schema(), "ts");
        assertEquals(plan.fromMs(), 100L);
        assertEquals(plan.toMs(), 200L);
        // Only deptId survives as a row predicate; ts was consumed by the date window.
        assertEquals(plan.predicate().columns().size(), 1);
    }

    @Test
    public void runsPushdownScanEndToEnd() throws Exception {
        // WHERE deptId = 1 AND salary >= 300 -> id=2 (400) and id=4 (900); other depts pruned.
        List<Object[]> rows = engine().executeSql(
                "SELECT id, salary FROM employee WHERE deptId = 1 AND salary >= 300", schema(), null);
        assertEquals(rows.size(), 2);
        boolean has400 = false;
        boolean has900 = false;
        for (Object[] r : rows) {
            assertEquals(r.length, 2, "projection keeps id, salary only");
            long salary = (Long) r[1];
            has400 |= salary == 400L;
            has900 |= salary == 900L;
        }
        assertTrue(has400 && has900);
    }

    @Test
    public void runsOrderByLimitTopK() throws Exception {
        List<Object[]> top = engine().executeSql(
                "SELECT id, salary FROM employee WHERE deptId = 1 AND salary >= 300 "
                        + "ORDER BY salary DESC LIMIT 1", schema(), null);
        assertEquals(top.size(), 1);
        assertEquals(top.get(0)[0], 4);        // id
        assertEquals(top.get(0)[1], 900L);     // salary
    }

    @Test
    public void strictAndInclusiveUpperBoundsDiffer() throws Exception {
        StreamLakeQueryExecutor engine = engine();
        // salary < 400 excludes the row equal to 400.
        List<Object[]> strict = engine.executeSql(
                "SELECT salary FROM employee WHERE deptId = 1 AND salary < 400", schema(), null);
        assertEquals(strict.size(), 1);
        assertEquals(strict.get(0)[0], 100L);

        // salary <= 400 includes it.
        List<Object[]> inclusive = engine().executeSql(
                "SELECT salary FROM employee WHERE deptId = 1 AND salary <= 400", schema(), null);
        assertEquals(inclusive.size(), 2);
    }

    @Test
    public void betweenAndInAreTranslated() throws Exception {
        // deptId IN (2, 3) -> pages for dept 2 (salary 500) and dept 3 (salary 50).
        List<Object[]> in = engine().executeSql(
                "SELECT id FROM employee WHERE deptId IN (2, 3)", schema(), null);
        assertEquals(in.size(), 2);

        // Plan-level: BETWEEN must yield exactly one column predicate that filters out-of-range rows.
        StreamLakeSqlPlanner.Plan bp = StreamLakeSqlPlanner.plan(
                "SELECT id FROM employee WHERE salary BETWEEN 100 AND 500", schema());
        assertEquals(bp.predicate().columns().size(), 1);
        assertTrue(bp.predicate().matchesRow(new Object[]{0, 0, 300L}));
        assertFalse(bp.predicate().matchesRow(new Object[]{0, 0, 900L}));
        assertFalse(bp.predicate().matchesRow(new Object[]{0, 0, 50L}));

        // salary BETWEEN 100 AND 500 across all depts -> ids 1 (100), 2 (400), 3 (500); excludes 900, 50.
        List<Object[]> between = engine().executeSql(
                "SELECT id FROM employee WHERE salary BETWEEN 100 AND 500", schema(), null);
        assertEquals(between.size(), 3);
    }

    @Test
    public void literalOnLeftFlipsOperator() throws Exception {
        // 300 <= salary is column >= 300; dept-1 rows 400 and 900 survive.
        List<Object[]> rows = engine().executeSql(
                "SELECT salary FROM employee WHERE deptId = 1 AND 300 <= salary", schema(), null);
        assertEquals(rows.size(), 2);
    }

    @Test
    public void caseInsensitiveColumnNames() {
        StreamLakeSqlPlanner.Plan plan = StreamLakeSqlPlanner.plan(
                "SELECT ID, SALARY FROM employee WHERE DEPTID = 1", schema());
        assertEquals(plan.projection(), new int[]{0, 2});
        assertFalse(plan.descending());
    }
}
