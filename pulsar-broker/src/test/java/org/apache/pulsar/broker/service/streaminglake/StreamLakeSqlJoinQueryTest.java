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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.streaminglake.StreamLakeProducer;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeTopicSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.StreamLakeQueryResult;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Proves the broker-side query stack runs an <b>inner-join SQL string</b> end to end and returns the
 * correct answer: {@link StreamLakeSqlPlanner#planStatement} parses the join, the
 * {@link StreamLakeQueryCoordinator} resolves each table to its per-topic {@link StreamLakeQueryService}
 * and runs {@code scanInnerJoin}, and the result count matches the closed-form expected value. This is
 * the same query the {@code pulsar-admin streamlake query} command submits over REST; here we call the
 * coordinator directly so the correctness of the SQL-join path is verified without the transport.
 */
public class StreamLakeSqlJoinQueryTest extends StreamLakeRealBookieTestBase {

    private static final long ROWS = 6000;
    private static final int ROWS_PER_PAGE = 20;

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        config.setManagedLedgerMaxEntriesPerLedger(50);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
    }

    private static StreamingLakeConfig cfg(List<StreamingLakeConfig.SchemaColumn> cols) {
        return StreamingLakeConfig.builder()
                .enabled(true).clientColumnarEnabled(true).setMaxCardinality(64).bloomFpp(0.01)
                .pageIndexEnsembleSize(1).pageIndexWriteQuorum(1).pageIndexAckQuorum(1)
                .segmentEnsembleSize(1).segmentWriteQuorum(1).segmentAckQuorum(1)
                .pageIndexMaxEntriesPerLedger(500).segmentMaxEntriesPerLedger(50)
                .columns(cols).build();
    }

    @Test(timeOut = 300_000)
    public void innerJoinSqlReturnsExpectedRows() throws Exception {
        String person = "persistent://" + NAMESPACE + "/Person";
        String employee = "persistent://" + NAMESPACE + "/Employee";
        admin.namespaces().setRetention(NAMESPACE, new RetentionPolicies(-1, -1));
        admin.topics().createNonPartitionedTopic(person);
        admin.topics().createNonPartitionedTopic(employee);

        register(person, cfg(Arrays.asList(
                new StreamingLakeConfig.SchemaColumn(1, "personId", "INT64", true),
                new StreamingLakeConfig.SchemaColumn(2, "name", "STRING", true),
                new StreamingLakeConfig.SchemaColumn(3, "age", "INT32", true))));
        register(employee, cfg(Arrays.asList(
                new StreamingLakeConfig.SchemaColumn(1, "empId", "INT64", true),
                new StreamingLakeConfig.SchemaColumn(2, "personId", "INT64", true),
                new StreamingLakeConfig.SchemaColumn(3, "salary", "INT64", true))));

        loadPerson(person);
        loadEmployee(employee);

        PersistentTopic pPerson = topic(person);
        Awaitility.await().atMost(120, TimeUnit.SECONDS).untilAsserted(() -> {
            long seg = pPerson.getStreamLakeSegmentService().catalog().all().values().stream()
                    .filter(i -> i.state == StreamLakeCatalog.State.SEGMENTED).count();
            assertTrue(seg >= 2, "expected segmented ledgers, was " + seg);
        });

        // Resolve a table name -> its per-topic query service (topic in the query's namespace).
        StreamLakeQueryCoordinator coordinator = new StreamLakeQueryCoordinator(table -> {
            PersistentTopic pt = topic("persistent://" + NAMESPACE + "/" + table);
            return pt == null ? null : pt.getStreamLakeQueryService();
        });

        String sql = "SELECT * FROM Person p JOIN Employee e ON p.personId = e.personId "
                + "WHERE p.age BETWEEN 30 AND 40 AND e.salary >= 40000";
        StreamLakeQueryResult res = coordinator.executeSql(sql);

        // Closed form: qualifies iff personId % 50 in [10, 20] (11 of every 50), over shared ids [0, ROWS).
        long expected = 0;
        for (long i = 0; i < ROWS; i++) {
            int band = (int) (i % 50);
            if (band >= 10 && band <= 20) {
                expected++;
            }
        }
        assertEquals(res.getRowCount(), (int) expected,
                "inner-join SQL must return the expected row count");
        assertEquals(res.getColumns(), Arrays.asList("Person.personId", "Person.name", "Person.age",
                "Employee.empId", "Employee.personId", "Employee.salary"),
                "SELECT * header is [left cols..., right cols...] with qualified names");
        // Every joined row shares the join key across the two sides.
        for (List<Object> row : res.getRows()) {
            assertEquals(row.get(0), row.get(4), "Person.personId must equal Employee.personId");
        }
        System.out.printf("%nSQL join returned %,d rows (expected %,d) in %,d ms%n",
                res.getRowCount(), expected, res.getLatencyMs());

        // Same query over the admin REST API (the transport behind `pulsar-admin streamlake query`).
        StreamLakeQueryResult viaRest = admin.streamLake().query(TENANT, "ns", sql);
        assertEquals(viaRest.getRowCount(), (int) expected,
                "inner-join over the admin REST endpoint must return the same count");
        assertEquals(viaRest.getColumns(), res.getColumns(),
                "REST result header must match the coordinator's");
        System.out.printf("SQL join over admin REST returned %,d rows in %,d ms%n",
                viaRest.getRowCount(), viaRest.getLatencyMs());
    }

    private void register(String topic, StreamingLakeConfig cfg) throws Exception {
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();
        Awaitility.await().untilAsserted(() -> assertTrue(topic(topic).isStreamLakeClientColumnar()));
    }

    private PersistentTopic topic(String name) {
        return (PersistentTopic) pulsar.getBrokerService().getTopicReference(name).orElse(null);
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

    private void loadPerson(String topic) throws Exception {
        StreamLakeTopicSchema ts = new StreamLakeTopicSchema(personSchema(), Arrays.asList(0, 1, 2), 64, 0.01);
        Producer<byte[]> raw = pulsarClient.newProducer().topic(topic)
                .enableBatching(false).compressionType(CompressionType.ZSTD).create();
        try (StreamLakeProducer p = new StreamLakeProducer(raw, ts, ROWS_PER_PAGE, 1 << 30, 0)) {
            for (long i = 0; i < ROWS; i++) {
                p.addRow(new Object[]{i, "person-" + i, 20 + (int) (i % 50)});
            }
            p.flush();
        }
        raw.close();
    }

    private void loadEmployee(String topic) throws Exception {
        StreamLakeTopicSchema ts = new StreamLakeTopicSchema(employeeSchema(), Arrays.asList(0, 1, 2), 64, 0.01);
        Producer<byte[]> raw = pulsarClient.newProducer().topic(topic)
                .enableBatching(false).compressionType(CompressionType.ZSTD).create();
        try (StreamLakeProducer p = new StreamLakeProducer(raw, ts, ROWS_PER_PAGE, 1 << 30, 0)) {
            for (long i = 0; i < ROWS; i++) {
                p.addRow(new Object[]{9_000_000_000L + i, i, 30_000L + (i % 50) * 1000L});
            }
            p.flush();
        }
        raw.close();
    }
}
