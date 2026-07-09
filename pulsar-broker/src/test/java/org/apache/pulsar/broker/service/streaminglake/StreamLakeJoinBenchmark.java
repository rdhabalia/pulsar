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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
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
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Inner-join benchmark: Person ⋈ Employee ON personId over a 10 GB / 2-day dataset with a data-ledger
 * rollover every 5 minutes (576 ledgers/table), so a time-windowed query prunes by event time and a
 * clustered predicate prunes pages. Metadata (catalog + page-index footers + segments) is built for
 * the full layout; data pages are synthesized on demand so surviving pages carry real rows without
 * physically storing 10 GB. The build side spills off-heap to {@code temp/}. Reports pruning %, IO
 * amplification, late-materialization, spill size and latencies.
 *
 * <p>Run (config via env vars, which Gradle forwards to the test JVM; {@code -D} is NOT forwarded):
 * {@code SL_BENCH_RUN=true SL_BENCH_TOTALGB=10 ./gradlew :pulsar-broker:test --tests "*StreamLakeJoinBenchmark"
 * -x checkstyleMain -x checkstyleTest}. Overridable: {@code SL_BENCH_TOTALGB / DAYS / ROLLOVERMIN /
 * WINDOWHOURS / BUCKETHI / SPILLDIR}. Not a CI test: skipped unless {@code SL_BENCH_RUN=true}.
 */
public class StreamLakeJoinBenchmark {

    private static final long MIB = 1024 * 1024;
    private static final long PAGE_BYTES = 1 * MIB;
    private static final int ROW_BYTES = 1024;
    private static final int ROWS_PER_PAGE = (int) (PAGE_BYTES / ROW_BYTES); // 1024
    private static final long PERSON_LEDGER_BASE = 1_000L;
    private static final long EMP_LEDGER_BASE = 1_000_000L;

    // Config from env (Gradle forwards the process environment to forked test JVMs; -D is not).
    private static long cfg(String name, long def) {
        String v = System.getenv("SL_BENCH_" + name);
        return v != null ? Long.parseLong(v.trim()) : def;
    }

    private static StreamLakeSchema personSchema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("personId", StreamLakeType.INT64),
                new StreamLakeSchema.Column("name", StreamLakeType.STRING),
                new StreamLakeSchema.Column("age", StreamLakeType.INT32)));
    }

    private static StreamLakeSchema employeeSchema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("empId", StreamLakeType.INT64),
                new StreamLakeSchema.Column("personId", StreamLakeType.INT64),
                new StreamLakeSchema.Column("salary", StreamLakeType.INT64)));
    }

    // Clustering bucket for a global page: drives the page's predicate band (age/salary) so a value
    // predicate prunes whole pages, and Person/Employee page g share it (their personId ranges match).
    private static int bucket(long globalPage) {
        return (int) (globalPage % 60);
    }

    // age of Person (page g, row k): a narrow band per page so an age range prunes pages.
    private static int personAge(long globalPage, int k) {
        return 20 + bucket(globalPage) + (k % 3);
    }

    private static long employeeSalary(long globalPage, int k) {
        return 30_000L + bucket(globalPage) * 1000L + (k % 3) * 100L;
    }

    // Person page g, row k -> full row [personId, name, age]; Employee references the same personId.
    private static Object[] personRow(long globalPage, int k) {
        long personId = globalPage * ROWS_PER_PAGE + k;
        return new Object[]{personId, "person-" + personId, personAge(globalPage, k)};
    }

    private static Object[] employeeRow(long globalPage, int k) {
        long personId = globalPage * ROWS_PER_PAGE + k;
        long empId = 9_000_000_000L + globalPage * ROWS_PER_PAGE + k;
        return new Object[]{empId, personId, employeeSalary(globalPage, k)};
    }

    @Test
    public void benchmark() throws Exception {
        if (!"true".equalsIgnoreCase(System.getenv("SL_BENCH_RUN"))) {
            return; // opt-in: only runs with SL_BENCH_RUN=true
        }
        long totalGb = cfg("TOTALGB", 10);
        long days = cfg("DAYS", 2);
        long rolloverMin = cfg("ROLLOVERMIN", 5);
        long windowHours = cfg("WINDOWHOURS", 8);
        int bucketHi = (int) cfg("BUCKETHI", 30);
        String spillDir = System.getenv("SL_BENCH_SPILLDIR") != null
                ? System.getenv("SL_BENCH_SPILLDIR")
                : "/Users/radhabal/in/git/streambricks/pulsar/pulsar/temp";

        long ledgers = days * 24 * 60 / rolloverMin;               // 576
        long tableBytes = totalGb * 1024 * MIB / 2;                // half each table
        long pagesPerTable = tableBytes / PAGE_BYTES;
        int pagesPerLedger = (int) Math.max(1, pagesPerTable / ledgers);
        long t0 = 1_700_000_000_000L;
        long ledgerMs = rolloverMin * 60_000L;
        long windowFromMs = t0;
        long windowToMs = t0 + windowHours * 3_600_000L;           // first `windowHours` of the 2 days
        // Predicate: bucket in [0, 30] -> age in [20,52], salary in [30000, 63000] (same pages both sides).
        OrderedExecutor exec = OrderedExecutor.newBuilder().numThreads(2).name("sl-bench").build();
        PulsarMockBookKeeper bk = new PulsarMockBookKeeper(exec);
        MetadataStore store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());

        StringBuilder rpt = new StringBuilder();
        rpt.append("\n===== StreamLake inner-join benchmark =====\n");
        rpt.append(String.format("dataset: %d GB, %d days, rollover %d min -> %d ledgers/table, "
                        + "%d pages/ledger, %d rows/page (%d KB rows, %d MB pages)%n",
                totalGb, days, rolloverMin, ledgers, pagesPerLedger, ROWS_PER_PAGE, ROW_BYTES / 1024,
                PAGE_BYTES / MIB));
        long totalPagesTable = (long) pagesPerLedger * ledgers;
        rpt.append(String.format("layout: %,d pages/table, %,d rows/table, %,d total rows%n",
                totalPagesTable, totalPagesTable * ROWS_PER_PAGE, 2 * totalPagesTable * ROWS_PER_PAGE));
        rpt.append(String.format("query: join on personId over a %d-hour window (ledgers ~%d/%d), "
                        + "age<=52 / salary<=63000 (bucket<=%d)%n",
                windowHours, windowHours * 60 / rolloverMin, ledgers, bucketHi));

        // ---- build metadata for both tables (footers index only the predicate column) ----
        long tMeta = System.nanoTime();
        Topic person = buildTopic(bk, store, "tenant/ns/persistent/person", personSchema(), true,
                PERSON_LEDGER_BASE, ledgers, pagesPerLedger, t0, ledgerMs);
        Topic employee = buildTopic(bk, store, "tenant/ns/persistent/employee", employeeSchema(), false,
                EMP_LEDGER_BASE, ledgers, pagesPerLedger, t0, ledgerMs);
        rpt.append(String.format("metadata build: %,d ms%n", (System.nanoTime() - tMeta) / 1_000_000));

        // ---- predicates ----
        StreamLakeScanPredicate personPred = StreamLakeScanPredicate.builder()
                .range(2, StreamLakeType.INT32, null, true, 20 + bucketHi + 2, true).build();
        StreamLakeScanPredicate empPred = StreamLakeScanPredicate.builder()
                .range(2, StreamLakeType.INT64, null, true, 30_000L + bucketHi * 1000L + 200L, true).build();

        // ---- run the join (build = Person, probe = Employee). Off-heap by default; SL_BENCH_ONHEAP=true
        // additionally runs an on-heap pass (guard heap: only for small windows). ----
        boolean[] offHeapModes = "true".equalsIgnoreCase(System.getenv("SL_BENCH_ONHEAP"))
                ? new boolean[]{false, true} : new boolean[]{true};
        for (boolean offHeap : offHeapModes) {
            person.reader.reset();
            employee.reader.reset();
            StreamLakeJoinTable table = offHeap
                    ? new SpillingJoinTable(Long.MAX_VALUE, spillDir)
                    : new OnHeapJoinTable(Long.MAX_VALUE);
            long t1 = System.nanoTime();
            List<Object[]> joined = person.executor.scanInnerJoin(windowFromMs, windowToMs,
                    personPred, 0, employee.executor, empPred, 1, table);
            long queryMs = (System.nanoTime() - t1) / 1_000_000;
            long spillBytes = offHeap ? ((SpillingJoinTable) table).spilledBytes() : 0;

            rpt.append(String.format("%n--- join (%s build table) ---%n",
                    offHeap ? "OFF-HEAP spill -> " + spillDir : "on-heap"));
            appendPrune(rpt, "Person(build)", person, totalPagesTable);
            appendPrune(rpt, "Employee(probe)", employee, totalPagesTable);
            long pagesRead = person.reader.pages.get() + employee.reader.pages.get();
            long bytesRead = pagesRead * PAGE_BYTES;
            long bytesTotal = 2 * totalPagesTable * PAGE_BYTES;
            rpt.append(String.format("IO: read %,d pages (%.1f MB) of %,d total (%.1f GB) -> "
                            + "%.3f%% scanned = %.0fx less read IO%n",
                    pagesRead, bytesRead / (double) MIB, 2 * totalPagesTable,
                    bytesTotal / (double) (1024 * MIB),
                    100.0 * bytesRead / bytesTotal, bytesTotal / (double) Math.max(1, bytesRead)));
            rpt.append(String.format("join: build rows=%,d, output rows=%,d", table.size(), joined.size()));
            if (offHeap) {
                rpt.append(String.format(", build spilled off-heap=%,d bytes (%.1f MB)",
                        spillBytes, spillBytes / (double) MIB));
            }
            rpt.append(String.format("%nquery latency: %,d ms%n", queryMs));
            table.close();
        }

        System.out.println(rpt);
        store.close();
        bk.shutdown();
        exec.shutdownNow();
    }

    private void appendPrune(StringBuilder rpt, String label, Topic t, long totalPagesTable) {
        rpt.append(String.format("  %-16s pages read=%,d / %,d (%.2f%%)%n", label,
                t.reader.pages.get(), totalPagesTable, 100.0 * t.reader.pages.get() / totalPagesTable));
    }

    // Build the catalog + page-index footers + segments for a topic's full layout.
    private Topic buildTopic(PulsarMockBookKeeper bk, MetadataStore store, String name,
            StreamLakeSchema schema, boolean isPerson, long ledgerBase, long ledgers, int pagesPerLedger,
            long t0, long ledgerMs) throws Exception {
        ManagedLedger ml = Mockito.mock(ManagedLedger.class);
        Mockito.when(ml.getName()).thenReturn(name);
        Mockito.when(ml.getProperties()).thenReturn(new HashMap<>());
        StreamLakeMetaStore ms = new StreamLakeMetaStore(store, ml);
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, ml, ms);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, ml, ms);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, ms);
        StreamLakeSegmentBuilder builder = new StreamLakeSegmentBuilder(pageIndex, segStore, catalog,
                2L * 1024 * 1024, 0.01);

        int predicateCol = 2; // age (Person) / salary (Employee)
        for (long i = 0; i < ledgers; i++) {
            long dataLedgerId = ledgerBase + i;
            for (int j = 0; j < pagesPerLedger; j++) {
                long g = i * pagesPerLedger + j;
                // footer over predicate-column-only rows (cheap: no strings/join key materialized here).
                List<Object[]> statRows = new ArrayList<>(ROWS_PER_PAGE);
                for (int k = 0; k < ROWS_PER_PAGE; k++) {
                    Object v = isPerson ? (Object) personAge(g, k) : (Object) employeeSalary(g, k);
                    Object[] r = new Object[3];
                    r[predicateCol] = v;
                    statRows.add(r);
                }
                byte[] footer = StreamLakeStatsBuilder.build(schema, statRows,
                        java.util.Collections.singletonList(predicateCol), 64, 0.01).encode();
                pageIndex.appendFooter(dataLedgerId, j, footer);
            }
            long minEt = t0 + i * ledgerMs;
            catalog.upsert(new StreamLakeCatalog.LedgerInfo(dataLedgerId, minEt, minEt, minEt + ledgerMs - 1,
                    (long) pagesPerLedger * ROWS_PER_PAGE, StreamLakeCatalog.State.CLOSED));
            builder.buildForLedger(dataLedgerId);
        }

        CountingReader reader = new CountingReader(schema, isPerson, ledgerBase, pagesPerLedger);
        StreamLakePruner pruner = new StreamLakePruner(catalog, segStore, pageIndex);
        return new Topic(new StreamLakeQueryExecutor(pruner, reader), reader);
    }

    /** A page reader that regenerates a page's real rows on demand and counts pages/bytes read. */
    private static final class CountingReader implements StreamLakeQueryExecutor.PageReader {
        final AtomicLong pages = new AtomicLong();
        private final StreamLakeSchema schema;
        private final boolean isPerson;
        private final long ledgerBase;
        private final int pagesPerLedger;

        CountingReader(StreamLakeSchema schema, boolean isPerson, long ledgerBase, int pagesPerLedger) {
            this.schema = schema;
            this.isPerson = isPerson;
            this.ledgerBase = ledgerBase;
            this.pagesPerLedger = pagesPerLedger;
        }

        void reset() {
            pages.set(0);
        }

        @Override
        public byte[] readArrowBatch(long ledgerId, long entryId) {
            pages.incrementAndGet();
            long ledgerIndex = ledgerId - ledgerBase;
            long g = ledgerIndex * pagesPerLedger + entryId;
            List<Object[]> rows = new ArrayList<>(ROWS_PER_PAGE);
            for (int k = 0; k < ROWS_PER_PAGE; k++) {
                rows.add(isPerson ? personRow(g, k) : employeeRow(g, k));
            }
            try (StreamLakeArrowBatchEncoder enc = new StreamLakeArrowBatchEncoder(schema)) {
                return enc.encode(rows);
            }
        }
    }

    private static final class Topic {
        final StreamLakeQueryExecutor executor;
        final CountingReader reader;

        Topic(StreamLakeQueryExecutor executor, CountingReader reader) {
            this.executor = executor;
            this.reader = reader;
        }
    }
}
