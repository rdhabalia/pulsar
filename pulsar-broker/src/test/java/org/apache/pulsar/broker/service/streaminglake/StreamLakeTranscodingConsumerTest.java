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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Slice 3B: a normal consumer on a batched StreamLake topic receives the original messages,
 * reconstructed (transcoded) from the stored columnar page entries -- with payloads and
 * properties intact. Verified on a real broker + real bookie.
 */
public class StreamLakeTranscodingConsumerTest extends StreamLakeRealBookieTestBase {

    private static final short DEPT = 1;
    private static final int N = 100;

    @Test(timeOut = 180_000)
    public void consumerReceivesOriginalMessagesDecodedFromPages() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sl-transcode";
        admin.topics().createNonPartitionedTopic(topic);

        StreamingLakeConfig cfg = StreamingLakeConfig.builder()
                .enabled(true).batchingEnabled(true)
                .maxPageMessages(25).pageGroupingDelayMs(5_000).pageSizeBytes(8 * 1024 * 1024)
                .indexedColumns(Arrays.asList(StreamingLakeConfig.IndexedColumn.builder()
                        .columnId(DEPT).name("departmentId").type("INT").build()))
                .build();
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();

        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(pt.isStreamLakeBatched()));

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("s").subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            int dept = i < 50 ? 5 : 15;
            sends.add(producer.newMessage()
                    .property("departmentId", String.valueOf(dept))
                    .key("k-" + i)
                    .value(("person-" + i).getBytes())
                    .sendAsync());
        }
        for (CompletableFuture<MessageId> f : sends) {
            f.get(60, TimeUnit.SECONDS);
        }

        // consumer receives every original message, transcoded out of the columnar pages
        Map<String, String> receivedDept = new HashMap<>();
        Map<String, String> receivedKey = new HashMap<>();
        for (int i = 0; i < N; i++) {
            Message<byte[]> m = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(m, "missing message " + i);
            String value = new String(m.getValue());
            receivedDept.put(value, m.getProperty("departmentId"));
            receivedKey.put(value, m.getKey());
            consumer.acknowledge(m);
        }

        assertEquals(receivedDept.size(), N, "consumer must receive all original messages");
        for (int i = 0; i < N; i++) {
            String value = "person-" + i;
            assertEquals(receivedDept.get(value), String.valueOf(i < 50 ? 5 : 15),
                    "property preserved for " + value);
            assertEquals(receivedKey.get(value), "k-" + i, "key preserved for " + value);
        }

        producer.close();
        consumer.close();
    }
}
