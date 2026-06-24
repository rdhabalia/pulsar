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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Smoke test confirming a real embedded Pulsar broker + BookKeeper runs in this
 * environment: a producer publishes Person records and a consumer receives them all.
 * This is the harness the real Streaming Lake integration will be built and validated on.
 */
public class StreamingLakeBrokerSmokeTest extends ProducerConsumerBase {

    private static final String TOPIC = "persistent://my-property/my-ns/persons";

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
    public void produceAndConsumePersons() throws Exception {
        final int numRecords = 1000;

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(TOPIC)
                .subscriptionName("persons-sub")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(TOPIC)
                .create();

        // produce: name,departmentId,salary
        for (int i = 0; i < numRecords; i++) {
            int dept = (i % 20) + 1;
            int salary = 10 + (i % 30) * 10;
            String payload = "person-" + i + "," + dept + "," + salary;
            producer.newMessage()
                    .eventTime(System.currentTimeMillis())
                    .property("departmentId", String.valueOf(dept))
                    .property("salary", String.valueOf(salary))
                    .value(payload.getBytes(StandardCharsets.UTF_8))
                    .send();
        }

        int received = 0;
        for (int i = 0; i < numRecords; i++) {
            Message<byte[]> msg = consumer.receive(10, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }
            received++;
            consumer.acknowledge(msg);
        }

        assertEquals(received, numRecords, "consumer received all produced Person records");

        producer.close();
        consumer.close();
    }
}
