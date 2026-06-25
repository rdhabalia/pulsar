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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Runnable end-to-end StreamLake demo on an in-process standalone cluster (real broker + real
 * bookie). Publishes 1000 Person records across 10 date partitions, then:
 *   - a normal consumer drains the whole topic and writes every Person to one file;
 *   - a StreamLake predicate query (date range + departmentId + salary) writes the matches to
 *     a second file, using broker date-partition pruning + bookie PAGE_PRUNE + selective decode.
 *
 * Output directory: -Dstreamlake.outDir (default /tmp/streamlake-out).
 */
public class StreamLakeClusterDemo extends StreamLakeRealBookieTestBase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final short COL_DEPT = 1;
    private static final short COL_SALARY = 2;
    private static final short COL_DAY = 3;
    private static final int TOTAL = 1000;
    private static final int PER_DAY = 100;
    private static final long BASE = 1_700_000_000_000L;
    private static final long DAY = 24L * 3600 * 1000;

    // query: dates in [day3, day6], departmentId in (5,15), salary > 50000
    private static final int FROM_DAY = 3;
    private static final int TO_DAY = 6;
    private static final int DEPT_GT = 5;
    private static final int DEPT_LT = 15;
    private static final int SALARY_GT = 50_000;

    public static final class Person {
        public String name;
        public int departmentId;
        public int salary;

        public Person() {
        }

        public Person(String name, int departmentId, int salary) {
            this.name = name;
            this.departmentId = departmentId;
            this.salary = salary;
        }

        @Override
        public String toString() {
            return "Person{name=" + name + ", departmentId=" + departmentId + ", salary=" + salary + "}";
        }
    }

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        // keep all pages in one ledger; date filtering here is exact via the day column,
        // while ledger-level date-partition pruning is covered by StreamLakeDatePruneTest.
        config.setManagedLedgerMaxEntriesPerLedger(100_000);
    }

    @Test(timeOut = 300_000)
    public void run() throws Exception {
        String dir = System.getenv("STREAMLAKE_OUT_DIR");
        if (dir == null) {
            dir = System.getProperty("streamlake.outDir", "/tmp/streamlake-out");
        }
        Path outDir = Paths.get(dir);
        Files.createDirectories(outDir);
        Path allFile = outDir.resolve("all-persons.txt");
        Path filteredFile = outDir.resolve("filtered-persons.txt");

        String topic = "persistent://" + NAMESPACE + "/person-demo";
        admin.topics().createNonPartitionedTopic(topic);

        // mark the topic StreamLake with indexed columns departmentId + salary
        StreamingLakeConfig cfg = StreamingLakeConfig.builder()
                .enabled(true).batchingEnabled(true)
                .maxPageMessages(PER_DAY).pageGroupingDelayMs(5_000).pageSizeBytes(16 * 1024 * 1024)
                .indexedColumns(Arrays.asList(
                        StreamingLakeConfig.IndexedColumn.builder()
                                .columnId(COL_DEPT).name("departmentId").type("INT").build(),
                        StreamingLakeConfig.IndexedColumn.builder()
                                .columnId(COL_SALARY).name("salary").type("INT").build(),
                        StreamingLakeConfig.IndexedColumn.builder()
                                .columnId(COL_DAY).name("day").type("INT").build()))
                .build();
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();

        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        Awaitility.await().untilAsserted(() ->
                org.testng.Assert.assertTrue(pt.isStreamLakeBatched()));

        // consumer first so it sees the whole stream
        Consumer<Person> consumer = pulsarClient.newConsumer(Schema.JSON(Person.class)).topic(topic)
                .subscriptionName("all").subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        // ----- produce 1000 Person records across 10 date partitions -----
        Producer<Person> producer = pulsarClient.newProducer(Schema.JSON(Person.class)).topic(topic)
                .enableBatching(false).create();
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < TOTAL; i++) {
            int day = i / PER_DAY;
            int departmentId = (i % 20) + 1;
            int salary = 30_000 + (i % 50) * 2_000;
            Person person = new Person("person-" + i, departmentId, salary);
            sends.add(producer.newMessage()
                    .property("departmentId", String.valueOf(departmentId))
                    .property("salary", String.valueOf(salary))
                    .property("day", String.valueOf(day))
                    .eventTime(BASE + day * DAY)
                    .value(person)
                    .sendAsync());
        }
        for (CompletableFuture<MessageId> f : sends) {
            f.get(60, TimeUnit.SECONDS);
        }

        // ----- consumer #1: drain everything, write each Person -----
        List<Person> all = new ArrayList<>();
        for (int i = 0; i < TOTAL; i++) {
            Message<Person> m = consumer.receive(30, TimeUnit.SECONDS);
            all.add(m.getValue());
            consumer.acknowledge(m);
        }
        all.sort(Comparator.comparingInt(p -> Integer.parseInt(p.name.substring("person-".length()))));
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(allFile, StandardCharsets.UTF_8))) {
            w.println("# StreamLake demo: all " + all.size() + " Person records (full consumer)");
            for (Person p : all) {
                w.println(p);
            }
        }

        // ----- consumer #2: StreamLake predicate query -----
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        BookKeeper bk = new BookKeeper(bkConf);
        List<Person> matched = new ArrayList<>();
        StreamLakePageScan.ScanResult scan;
        try {
            // date partition [day3,day6] (exact, via the day column) + departmentId + salary;
            // the date window also drives the cheap ledger-level prune.
            List<StreamLakePageScan.Bound> bounds = Arrays.asList(
                    new StreamLakePageScan.Bound(COL_DAY, "day", FROM_DAY - 1, TO_DAY + 1),
                    new StreamLakePageScan.Bound(COL_DEPT, "departmentId", DEPT_GT, DEPT_LT),
                    new StreamLakePageScan.Bound(COL_SALARY, "salary", SALARY_GT, null));
            scan = StreamLakePageScan.scan(pt, bk, bounds, BASE + FROM_DAY * DAY, BASE + TO_DAY * DAY);
            for (byte[] payload : scan.rows) {
                matched.add(MAPPER.readValue(payload, Person.class));
            }
        } finally {
            bk.close();
        }
        matched.sort(Comparator.comparingInt(p -> Integer.parseInt(p.name.substring("person-".length()))));

        // brute-force oracle for cross-checking the query result
        long oracle = all.stream().filter(p -> {
            int day = Integer.parseInt(p.name.substring("person-".length())) / PER_DAY;
            return day >= FROM_DAY && day <= TO_DAY
                    && p.departmentId > DEPT_GT && p.departmentId < DEPT_LT && p.salary > SALARY_GT;
        }).count();

        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(filteredFile, StandardCharsets.UTF_8))) {
            w.println("# StreamLake predicate query");
            w.println("# WHERE date_partition in [day" + FROM_DAY + ", day" + TO_DAY + "]"
                    + " AND departmentId > " + DEPT_GT + " AND departmentId < " + DEPT_LT
                    + " AND salary > " + SALARY_GT);
            w.println("# pruning: ledgers scanned=" + scan.ledgersScanned
                    + ", ledgers pruned by date=" + scan.ledgersPrunedByDate);
            w.println("# matches=" + matched.size() + " (brute-force oracle=" + oracle + ")");
            for (Person p : matched) {
                w.println(p);
            }
        }

        producer.close();
        consumer.close();

        org.testng.Assert.assertEquals(all.size(), TOTAL);
        org.testng.Assert.assertEquals(matched.size(), (int) oracle, "predicate query must equal oracle");
        org.testng.Assert.assertTrue(matched.size() > 0, "query should match some rows");
        System.out.println("STREAMLAKE_DEMO_ALL=" + allFile.toAbsolutePath());
        System.out.println("STREAMLAKE_DEMO_FILTERED=" + filteredFile.toAbsolutePath());
    }
}
