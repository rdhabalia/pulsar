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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Durability of the StreamLake date-partition index (design point 7): after the topic is
 * unloaded and reloaded, the per-ledger date ranges are rebuilt by replaying the durable
 * date-partition-list ledger -- not lost like an in-memory map. Verified on a real bookie.
 */
public class StreamLakeDateIndexDurabilityTest extends StreamLakeRealBookieTestBase {

    private static final short DEPT = 1;
    private static final int N = 100;
    private static final long DAY1 = 1_700_000_000_000L;
    private static final long DAY2 = DAY1 + 24L * 3600 * 1000;

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        config.setManagedLedgerMaxEntriesPerLedger(2);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
    }

    @Test(timeOut = 180_000)
    public void dateIndexSurvivesTopicReload() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sl-dateindex-durable";
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

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            long eventTime = i < 50 ? DAY1 : DAY2;
            sends.add(producer.newMessage()
                    .property("departmentId", String.valueOf(i < 50 ? 5 : 15))
                    .eventTime(eventTime)
                    .value(("person-" + i).getBytes())
                    .sendAsync());
        }
        for (CompletableFuture<MessageId> f : sends) {
            f.get(60, TimeUnit.SECONDS);
        }

        // snapshot the date index before unload
        Map<Long, long[]> before = new HashMap<>();
        pt.getStreamLakeDateIndex().forEach((k, v) -> before.put(k, new long[]{v[0], v[1]}));
        assertTrue(before.size() >= 2, "expected at least two date-partitioned ledgers, got " + before.size());

        // unload the topic -- an in-memory index would be lost here
        admin.topics().unload(topic);

        // reload by publishing again; the new batcher must replay the durable date-partition ledger
        producer.newMessage().property("departmentId", "5").eventTime(DAY1)
                .value("trigger".getBytes()).send();

        PersistentTopic reloaded = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        assertTrue(reloaded != pt, "topic should have been reloaded into a new instance");

        Map<Long, long[]> after = reloaded.getStreamLakeDateIndex();
        // every pre-unload ledger date range must be present after reload (rebuilt from the ledger)
        for (Map.Entry<Long, long[]> e : before.entrySet()) {
            long[] v = after.get(e.getKey());
            assertTrue(v != null, "ledger " + e.getKey() + " missing from rebuilt date index");
            assertEquals(v[0], e.getValue()[0], "minDate mismatch for ledger " + e.getKey());
            assertEquals(v[1], e.getValue()[1], "maxDate mismatch for ledger " + e.getKey());
        }

        producer.close();
    }
}
