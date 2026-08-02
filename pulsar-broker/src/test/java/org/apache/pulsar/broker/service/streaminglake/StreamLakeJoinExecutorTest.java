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
import java.util.TreeSet;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.client.streaminglake.OnHeapJoinTable;
import org.apache.pulsar.client.streaminglake.SpillingJoinTable;
import org.apache.pulsar.client.streaminglake.StreamLakeArrowBatchEncoder;
import org.apache.pulsar.client.streaminglake.StreamLakeJoinTable;
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

/**
 * End-to-end inner join across two StreamLake topics (Person ⋈ Employee on personId): each side prunes
 * date → segment → page over its own metadata, the build side fills the hash table, and the probe side
 * is late-materialized (full probe row built only on a key match). Verified identical for the on-heap
 * and the disk-spilling build tables.
 */
public class StreamLakeJoinExecutorTest {

    private static final long DAY1 = 1_700_000_000_000L;

    private OrderedExecutor executor;
    private PulsarMockBookKeeper bk;
    private MetadataStore store;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("sl-join-test").build();
        bk = new PulsarMockBookKeeper(executor);
        store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());
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

    private static StreamLakeSchema personSchema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("personId", StreamLakeType.INT32),
                new StreamLakeSchema.Column("name", StreamLakeType.STRING),
                new StreamLakeSchema.Column("age", StreamLakeType.INT32)));
    }

    private static StreamLakeSchema employeeSchema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("empId", StreamLakeType.INT32),
                new StreamLakeSchema.Column("personId", StreamLakeType.INT32),
                new StreamLakeSchema.Column("salary", StreamLakeType.INT64)));
    }

    // Build a query executor for a topic: append each page's footer, register + segment the ledger.
    private StreamLakeQueryExecutor executorFor(String mlName, StreamLakeSchema schema, long dataLedgerId,
            List<List<Object[]>> pages) throws Exception {
        ManagedLedger ml = mock(ManagedLedger.class);
        when(ml.getName()).thenReturn(mlName);
        when(ml.getProperties()).thenReturn(new HashMap<>());
        StreamLakeMetaStore ms = new StreamLakeMetaStore(store, ml);
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, ml, ms);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, ml, ms);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, ms);

        Map<Long, byte[]> pageBytes = new HashMap<>();
        for (int i = 0; i < pages.size(); i++) {
            try (StreamLakeArrowBatchEncoder enc = new StreamLakeArrowBatchEncoder(schema)) {
                pageBytes.put((long) i, enc.encode(pages.get(i)));
            }
            byte[] footer = StreamLakeStatsBuilder.build(schema, pages.get(i), Arrays.asList(0, 1, 2), 64, 0.01)
                    .encode();
            pageIndex.appendFooter(dataLedgerId, i, footer);
        }
        catalog.upsert(new StreamLakeCatalog.LedgerInfo(dataLedgerId, 1L, DAY1, DAY1 + 3600_000,
                pages.size(), StreamLakeCatalog.State.CLOSED));
        new StreamLakeSegmentBuilder(pageIndex, segStore, catalog, 2L * 1024 * 1024, 0.01)
                .buildForLedger(dataLedgerId);

        StreamLakePruner pruner = new StreamLakePruner(catalog, segStore, pageIndex);
        return new StreamLakeQueryExecutor(pruner, (lid, eid) -> pageBytes.get(eid));
    }

    private final java.util.concurrent.atomic.AtomicInteger topicSeq =
            new java.util.concurrent.atomic.AtomicInteger();

    private StreamLakeQueryExecutor personExecutor() throws Exception {
        return executorFor("tenant/ns/persistent/person-" + topicSeq.incrementAndGet(),
                personSchema(), 100L, Arrays.asList(
                Arrays.asList(new Object[]{1, "alice", 40}, new Object[]{2, "bob", 50}),
                Arrays.asList(new Object[]{3, "carol", 60}, new Object[]{4, "dave", 70}),
                java.util.Collections.singletonList(new Object[]{5, "eve", 80})));
    }

    private StreamLakeQueryExecutor employeeExecutor() throws Exception {
        return executorFor("tenant/ns/persistent/employee-" + topicSeq.incrementAndGet(),
                employeeSchema(), 200L, Arrays.asList(
                Arrays.asList(new Object[]{10, 1, 100L}, new Object[]{11, 2, 200L}),
                Arrays.asList(new Object[]{12, 3, 300L}, new Object[]{13, 4, 400L}),
                java.util.Collections.singletonList(new Object[]{14, 6, 500L})));
    }

    // build = Person (age in [50,70]) keyed on personId(0); probe = Employee (salary>=200) keyed on personId(1).
    private List<Object[]> runJoin(StreamLakeJoinTable buildTable) throws Exception {
        StreamLakeQueryExecutor person = personExecutor();
        StreamLakeQueryExecutor employee = employeeExecutor();
        StreamLakeScanPredicate personPred = StreamLakeScanPredicate.builder()
                .range(2, StreamLakeType.INT32, 50, 70).build();
        StreamLakeScanPredicate empPred = StreamLakeScanPredicate.builder()
                .range(2, StreamLakeType.INT64, 200L, null).build();
        // build = Person (smaller after pruning); probe = Employee. Emits concat(employeeRow, personRow).
        return person.scanInnerJoin(DAY1, DAY1 + 1000, personPred, 0, employee, empPred, 1, buildTable);
    }

    private void assertJoinCorrect(List<Object[]> joined) {
        // build side = Person; probe side = Employee; scanInnerJoin emits concat(probeRow, buildRow):
        // [empId, empPersonId, salary, personId, name, age]. Persons 2,3,4 match employees 11,12,13.
        assertEquals(joined.size(), 3);
        TreeSet<String> names = new TreeSet<>();
        for (Object[] r : joined) {
            assertEquals(r.length, 6, "concat(employee[3], person[3])");
            assertEquals(r[1], r[3], "join key personId matches on both sides");
            names.add((String) r[4]);
            assertTrue(((Long) r[2]) >= 200L, "employee salary predicate held");
            int age = (Integer) r[5];
            assertTrue(age >= 50 && age <= 70, "person age predicate held");
        }
        assertEquals(names, new TreeSet<>(Arrays.asList("bob", "carol", "dave")));
    }

    @Test
    public void innerJoinOnHeap() throws Exception {
        assertJoinCorrect(runJoin(new OnHeapJoinTable(Long.MAX_VALUE)));
    }

    @Test
    public void innerJoinOffHeapSpilling() throws Exception {
        try (StreamLakeJoinTable spill = new SpillingJoinTable(Long.MAX_VALUE, "")) {
            assertJoinCorrect(runJoin(spill));
        }
    }

    @Test
    public void bothBackendsProduceIdenticalResults() throws Exception {
        List<Object[]> heap = runJoin(new OnHeapJoinTable(Long.MAX_VALUE));
        List<Object[]> spill;
        try (StreamLakeJoinTable t = new SpillingJoinTable(Long.MAX_VALUE, "")) {
            spill = runJoin(t);
        }
        assertEquals(spill.size(), heap.size());
        assertEquals(sortedKeys(spill), sortedKeys(heap));
    }

    private static List<Integer> sortedKeys(List<Object[]> rows) {
        List<Integer> keys = new ArrayList<>();
        for (Object[] r : rows) {
            keys.add((Integer) r[1]);
        }
        keys.sort(null);
        return keys;
    }
}
