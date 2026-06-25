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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Slice 1: marking a topic as StreamLake via topic policies is read by the broker
 * (PersistentTopic.isStreamLakeEnabled). Runs against a real broker + real bookie.
 */
public class StreamLakePolicyTest extends StreamLakeRealBookieTestBase {

    @Test(timeOut = 120_000)
    public void brokerReadsStreamLakeFlag() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sl-flag";
        admin.topics().createNonPartitionedTopic(topic);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        assertFalse(pt.isStreamLakeEnabled(), "topic should not be StreamLake by default");

        StreamingLakeConfig cfg = StreamingLakeConfig.builder()
                .enabled(true)
                .pageSizeBytes(2 * 1024 * 1024)
                .build();
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                policies -> policies.setStreamingLake(cfg)).get();

        // policy update propagates to the topic asynchronously
        Awaitility.await().untilAsserted(() ->
                assertTrue(pt.isStreamLakeEnabled(), "broker should see the StreamLake flag"));

        producer.close();
    }
}
