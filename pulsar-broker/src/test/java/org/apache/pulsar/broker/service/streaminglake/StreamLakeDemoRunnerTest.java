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

import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.streaminglake.StreamLakeBatchPayload;
import org.apache.pulsar.client.streaminglake.StreamLakeProducer;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeTopicSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.apache.pulsar.client.streaminglake.OnHeapJoinTable;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Runnable end-to-end StreamLake demo on a real broker + bookie. Opt-in via {@code SL_DEMO_RUN=true}:
 * registers Person + Employee StreamLake tables, generates a configurable load (which -- with small
 * ledger rollovers -- creates many data / page-index / segment ledgers), waits for the owning broker to
 * auto-build the catalog + segments, lists the resulting ledgers for validation, then runs the inner
 * join {@code Person &#8904; Employee ON personId} reading real data pages from the managed ledger, and
 * prints the results + the expected answer.
 *
 * <p>Env knobs: {@code SL_DEMO_PERSON_ROWS} (default 40000), {@code SL_DEMO_EMP_ROWS} (default 40000),
 * {@code SL_DEMO_ROWS_PER_PAGE} (default 20). See {@code streamLake/demo/demo.md}.
 */
public class StreamLakeDemoRunnerTest extends StreamLakeRealBookieTestBase {

    private static long env(String k, long def) {
        String v = System.getenv(k);
        return v != null ? Long.parseLong(v.trim()) : def;
    }

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        // Small data-ledger rollover so the demo creates MANY data ledgers (and thus page-index and
        // segment ledgers) from a modest row count -- the shape a large deployment has at scale.
        config.setManagedLedgerMaxEntriesPerLedger(50);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
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

    private static StreamingLakeConfig config(List<StreamingLakeConfig.SchemaColumn> cols) {
        return StreamingLakeConfig.builder()
                .enabled(true).clientColumnarEnabled(true).setMaxCardinality(64).bloomFpp(0.01)
                // single-bookie demo: RF 1/1/1 for every StreamLake ledger
                .pageIndexEnsembleSize(1).pageIndexWriteQuorum(1).pageIndexAckQuorum(1)
                .segmentEnsembleSize(1).segmentWriteQuorum(1).segmentAckQuorum(1)
                // small rollovers so multiple page-index / segment ledgers form (demonstrates the layout)
                .pageIndexMaxEntriesPerLedger(500).segmentMaxEntriesPerLedger(50)
                .columns(cols).build();
    }

    @Test(timeOut = 600_000)
    public void demo() throws Exception {
        if (!"true".equalsIgnoreCase(System.getenv("SL_DEMO_RUN"))) {
            return; // opt-in
        }
        long personRows = env("SL_DEMO_PERSON_ROWS", 40_000);
        long empRows = env("SL_DEMO_EMP_ROWS", 40_000);
        int rowsPerPage = (int) env("SL_DEMO_ROWS_PER_PAGE", 20);

        // StreamLake data ledgers must not be trimmed once closed (they hold the queryable pages);
        // keep them with infinite retention.
        admin.namespaces().setRetention(NAMESPACE,
                new org.apache.pulsar.common.policies.data.RetentionPolicies(-1, -1));

        StringBuilder rpt = new StringBuilder("\n===== StreamLake demo =====\n");
        String person = "persistent://" + NAMESPACE + "/Person";
        String employee = "persistent://" + NAMESPACE + "/Employee";
        admin.topics().createNonPartitionedTopic(person);
        admin.topics().createNonPartitionedTopic(employee);

        register(person, config(Arrays.asList(
                new StreamingLakeConfig.SchemaColumn(1, "personId", "INT64", true),
                new StreamingLakeConfig.SchemaColumn(2, "name", "STRING", true),
                new StreamingLakeConfig.SchemaColumn(3, "age", "INT32", true))));
        register(employee, config(Arrays.asList(
                new StreamingLakeConfig.SchemaColumn(1, "empId", "INT64", true),
                new StreamingLakeConfig.SchemaColumn(2, "personId", "INT64", true),
                new StreamingLakeConfig.SchemaColumn(3, "salary", "INT64", true))));

        long t0 = System.nanoTime();
        loadPerson(person, personRows, rowsPerPage);
        loadEmployee(employee, empRows, rowsPerPage);
        rpt.append(String.format("ingest: Person=%,d rows, Employee=%,d rows in %,d ms%n",
                personRows, empRows, (System.nanoTime() - t0) / 1_000_000));

        PersistentTopic pPerson = topic(person);
        PersistentTopic pEmployee = topic(employee);

        // Wait until the owning broker has auto-segmented the closed data ledgers.
        Awaitility.await().atMost(120, TimeUnit.SECONDS).untilAsserted(() -> {
            long seg = pPerson.getStreamLakeSegmentService().catalog().all().values().stream()
                    .filter(i -> i.state == StreamLakeCatalog.State.SEGMENTED).count();
            assertTrue(seg >= 2, "expected several segmented data ledgers, was " + seg);
        });

        appendLedgerReport(rpt, "Person", pPerson);
        appendLedgerReport(rpt, "Employee", pEmployee);

        // Inner join: Person age in [30,40] JOIN Employee salary >= 40000 ON personId.
        StreamLakeQueryExecutor personExec = executor(pPerson);
        StreamLakeQueryExecutor employeeExec = executor(pEmployee);
        StreamLakeScanPredicate personPred = StreamLakeScanPredicate.builder()
                .range(2, StreamLakeType.INT32, 30, true, 40, true).build();
        StreamLakeScanPredicate empPred = StreamLakeScanPredicate.builder()
                .range(2, StreamLakeType.INT64, 40_000L, true, null, true).build();
        long q0 = System.nanoTime();
        List<Object[]> joined = personExec.scanInnerJoin(0, Long.MAX_VALUE, personPred, 0,
                employeeExec, empPred, 1, new OnHeapJoinTable(Long.MAX_VALUE));
        long queryMs = (System.nanoTime() - q0) / 1_000_000;

        // Expected: personId shared by both; Person age = 20 + personId%50 in [30,40] -> personId%50 in
        // [10,20]; Employee salary = 30000 + personId%50 * 1000 >= 40000 -> personId%50 >= 10. So the
        // join keys are personIds present in BOTH tables with personId%50 in [10,20].
        long common = Math.min(personRows, empRows);
        long expected = 0;
        for (long p = 0; p < common; p++) {
            int band = (int) (p % 50);
            if (band >= 10 && band <= 20) {
                expected++;
            }
        }
        rpt.append(String.format("%nquery: Person(age 30..40) JOIN Employee(salary>=40000) ON personId%n"));
        rpt.append(String.format("  matches=%,d (expected=%,d)  latency=%,d ms%n",
                joined.size(), expected, queryMs));
        rpt.append(joined.size() == expected ? "  RESULT: OK\n" : "  RESULT: MISMATCH\n");
        if (!joined.isEmpty()) {
            Object[] r = joined.get(0);
            rpt.append(String.format("  sample row [empId,personId,salary,personId,name,age] = %s%n",
                    Arrays.toString(r)));
        }
        System.out.println(rpt);
        assertTrue(joined.size() == expected, "join result count must match expected");
    }

    private void register(String topic, StreamingLakeConfig cfg) throws Exception {
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();
        Awaitility.await().untilAsserted(() -> assertTrue(topic(topic).isStreamLakeClientColumnar()));
    }

    private PersistentTopic topic(String name) {
        return (PersistentTopic) pulsar.getBrokerService().getTopicReference(name).orElseThrow();
    }

    private void loadPerson(String topic, long rows, int rowsPerPage) throws Exception {
        StreamLakeTopicSchema ts = new StreamLakeTopicSchema(personSchema(), Arrays.asList(0, 1, 2), 64, 0.01);
        Producer<byte[]> raw = pulsarClient.newProducer().topic(topic)
                .enableBatching(false).compressionType(CompressionType.ZSTD).create();
        try (StreamLakeProducer p = new StreamLakeProducer(raw, ts, rowsPerPage, 1 << 30, 0)) {
            for (long i = 0; i < rows; i++) {
                p.addRow(new Object[]{i, "person-" + i, 20 + (int) (i % 50)});
            }
            p.flush();
        }
        raw.close();
    }

    private void loadEmployee(String topic, long rows, int rowsPerPage) throws Exception {
        StreamLakeTopicSchema ts = new StreamLakeTopicSchema(employeeSchema(), Arrays.asList(0, 1, 2), 64, 0.01);
        Producer<byte[]> raw = pulsarClient.newProducer().topic(topic)
                .enableBatching(false).compressionType(CompressionType.ZSTD).create();
        try (StreamLakeProducer p = new StreamLakeProducer(raw, ts, rowsPerPage, 1 << 30, 0)) {
            for (long i = 0; i < rows; i++) {
                p.addRow(new Object[]{9_000_000_000L + i, i, 30_000L + (i % 50) * 1000L});
            }
            p.flush();
        }
        raw.close();
    }

    private void appendLedgerReport(StringBuilder rpt, String label, PersistentTopic pt) throws Exception {
        StreamLakeMetaStore ms = new StreamLakeMetaStore(pulsar.getLocalMetadataStore(), pt.getManagedLedger());
        StreamLakeMetaStore.Record rec = ms.read();
        long dataLedgers = pt.getStreamLakeSegmentService().catalog().all().size();
        rpt.append(String.format("%s ledgers: data=%,d  pageIndex=%,d  segment=%,d  catalog=%s%n",
                label, dataLedgers, rec.pageIndexLedgerIds.size(), rec.segmentLedgerIds.size(),
                rec.catalogLedgerId));
    }

    private StreamLakeQueryExecutor executor(PersistentTopic pt) {
        StreamLakeSegmentService svc = pt.getStreamLakeSegmentService();
        StreamLakePruner pruner = new StreamLakePruner(svc.catalog(), svc.segmentStore(),
                pt.getStreamLakePageIndex());
        ManagedLedger ml = pt.getManagedLedger();
        StreamLakeQueryExecutor.PageReader reader = (ledgerId, entryId) -> {
            CompletableFuture<byte[]> f = new CompletableFuture<>();
            Position pos = PositionFactory.create(ledgerId, entryId);
            ml.asyncReadEntry(pos, new AsyncCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(Entry entry, Object ctx) {
                    try {
                        ByteBuf buf = entry.getDataBuffer();
                        Commands.parseMessageMetadata(buf); // advance reader index past the message metadata
                        byte[] payload = new byte[buf.readableBytes()];
                        buf.getBytes(buf.readerIndex(), payload);
                        f.complete(StreamLakeBatchPayload.arrowBatch(payload));
                    } catch (Throwable t) {
                        f.completeExceptionally(t);
                    } finally {
                        entry.release();
                    }
                }

                @Override
                public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                    f.completeExceptionally(exception);
                }
            }, null);
            try {
                return f.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return new StreamLakeQueryExecutor(pruner, reader);
    }
}
