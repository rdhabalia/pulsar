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
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Real broker + BookKeeper integration test of the Streaming Lake query:
 * a producer writes Person records to a topic; the broker-side
 * {@link StreamingLakeScanService} runs a server-side scan over the topic's managed
 * ledger for a date range + column predicate; results are validated against a
 * brute-force oracle. Runs on the embedded broker (MockedPulsarServiceBaseTest).
 */
public class StreamingLakeScanIntegrationTest extends ProducerConsumerBase {

    private static final String TOPIC = "persistent://my-property/my-ns/persons-scan";
    private static final long DAY = 86_400_000L;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void serverSideScanWithDatePartitionAndPredicate() throws Exception {
        final int numRecords = 1000;
        final int days = 5;
        final long day0 = LocalDate.of(2026, 6, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

        // ---- producer writes Person records (no batching -> 1 message per entry) ----
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(TOPIC)
                .enableBatching(false)
                .create();

        List<long[]> produced = new ArrayList<>(); // {eventTime, dept, salary}
        for (int i = 0; i < numRecords; i++) {
            int dayOffset = i % days;
            int dept = (i % 20) + 1;                 // 1..20
            int salary = 10 + (i % 30) * 10;         // 10..300
            long eventTime = day0 + dayOffset * DAY + i * 1000L; // stays within its day
            producer.newMessage()
                    .eventTime(eventTime)
                    .property("name", "person-" + i)
                    .property("departmentId", String.valueOf(dept))
                    .property("salary", String.valueOf(salary))
                    .value(("person-" + i).getBytes(StandardCharsets.UTF_8))
                    .send();
            produced.add(new long[]{eventTime, dept, salary});
        }
        producer.flush();

        // ---- get the topic's managed ledger from the broker ----
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(TOPIC).orElseThrow();
        ManagedLedger ledger = topic.getManagedLedger();

        // ---- query: date_partition in [day0, day1] AND deptId > 5 AND deptId < 15 AND salary > 100 ----
        final long from = day0;
        final long to = day0 + 2 * DAY - 1;          // day 0 and day 1 only
        final int deptX = 5;
        final int deptY = 15;
        final int salaryZ = 100;

        List<StreamingLakeScanService.Row> result =
                StreamingLakeScanService.scan(ledger, from, to, deptX, deptY, salaryZ);

        // ---- oracle ----
        int expected = 0;
        for (long[] p : produced) {
            long et = p[0];
            int dept = (int) p[1];
            int salary = (int) p[2];
            if (et >= from && et <= to && dept > deptX && dept < deptY && salary > salaryZ) {
                expected++;
            }
        }

        assertEquals(result.size(), expected, "server-side filtered scan matches the oracle");
        assertTrue(expected > 0, "sanity: the predicate should match some rows");
        for (StreamingLakeScanService.Row r : result) {
            assertTrue(r.eventTime >= from && r.eventTime <= to
                            && r.departmentId > deptX && r.departmentId < deptY && r.salary > salaryZ,
                    "every returned row satisfies the full predicate: " + r);
        }

        // ---- full scan (SELECT *) returns every record ----
        List<StreamingLakeScanService.Row> full = StreamingLakeScanService.scan(
                ledger, Long.MIN_VALUE, Long.MAX_VALUE,
                Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE);
        assertEquals(full.size(), numRecords, "full scan returns all produced records");

        producer.close();
    }
}
