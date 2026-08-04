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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.streaminglake.StreamLakeArrowBatchEncoder;
import org.apache.pulsar.client.streaminglake.StreamLakeBatchPayload;
import org.apache.pulsar.client.streaminglake.StreamLakeStatsBuilder;
import org.apache.pulsar.client.streaminglake.StreamLakeTopicSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Phase F (async segment build via a system topic): with
 * {@link StreamingLakeConfig#isAsyncSegmentBuildViaSystemTopic()} enabled, a closed data ledger's
 * segment is NOT built inline on the owning broker. Instead the broker publishes a
 * {@code {dataTopic, dataLedgerId}} request to the per-namespace {@code __streamlake_segment_build}
 * system topic, and a {@link StreamLakeSegmentBuildQueue} failover consumer resolves the builder and
 * builds the segment, then acks. This test proves the closed ledgers still reach
 * {@link StreamLakeCatalog.State#SEGMENTED} <b>and</b> that the build requests actually flowed through
 * the system topic (its published-message counter advances), i.e. the build was decoupled from the
 * pub-sub broker's hot path.
 */
public class StreamLakeAsyncSegmentBuildTest extends StreamLakeRealBookieTestBase {

    private static final int ROWS_PER_PAGE = 20;
    private static final int MAX_ENTRIES_PER_LEDGER = 4;
    private static final int PAGES = 13; // ledgers of 4,4,4,1 -> the first three close and get segmented

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        config.setManagedLedgerMaxEntriesPerLedger(MAX_ENTRIES_PER_LEDGER);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
    }

    private static StreamingLakeConfig streamLakeConfig() {
        return StreamingLakeConfig.builder()
                .enabled(true).clientColumnarEnabled(true)
                .setMaxCardinality(64).bloomFpp(0.01)
                // Route segment builds through the per-namespace system topic instead of inline.
                .asyncSegmentBuildViaSystemTopic(true)
                .pageIndexEnsembleSize(1).pageIndexWriteQuorum(1).pageIndexAckQuorum(1)
                .segmentEnsembleSize(1).segmentWriteQuorum(1).segmentAckQuorum(1)
                .columns(Arrays.asList(
                        new StreamingLakeConfig.SchemaColumn(1, "id", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(2, "deptId", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(3, "email", "STRING", true)))
                .build();
    }

    @Test(timeOut = 180_000)
    public void closingLedgersBuildSegmentsViaSystemTopic() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sl-async-segment";
        String systemTopic = "persistent://" + NAMESPACE + "/"
                + StreamLakeSegmentBuildQueue.SYSTEM_TOPIC;
        admin.topics().createNonPartitionedTopic(topic);

        StreamingLakeConfig cfg = streamLakeConfig();
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();

        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(pt.isStreamLakeClientColumnar()));

        StreamLakeTopicSchema schema = StreamLakeTopicSchema.fromConfig(cfg);
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        long[] pageLedger = new long[PAGES];
        try (StreamLakeArrowBatchEncoder encoder = new StreamLakeArrowBatchEncoder(schema.schema())) {
            for (int p = 0; p < PAGES; p++) {
                List<Object[]> rows = new ArrayList<>(ROWS_PER_PAGE);
                for (int r = 0; r < ROWS_PER_PAGE; r++) {
                    int id = p * ROWS_PER_PAGE + r;
                    rows.add(new Object[]{id, id % 4, "user-" + id + "@x.com"});
                }
                byte[] arrow = encoder.encode(rows);
                byte[] footer = StreamLakeStatsBuilder.build(schema.schema(), rows, schema.indexedColumns(),
                        schema.setMaxCardinality(), schema.bloomFpp()).encode();
                MessageId id = producer.send(StreamLakeBatchPayload.combine(arrow, footer));
                pageLedger[p] = ((MessageIdImpl) id).getLedgerId();
            }
        }
        producer.close();

        List<Long> ledgersInOrder = new ArrayList<>();
        for (long lid : pageLedger) {
            if (ledgersInOrder.isEmpty() || ledgersInOrder.get(ledgersInOrder.size() - 1) != lid) {
                ledgersInOrder.add(lid);
            }
        }
        assertTrue(ledgersInOrder.size() >= 3, "expected several ledger rollovers, got " + ledgersInOrder);
        List<Long> closedLedgers = ledgersInOrder.subList(0, ledgersInOrder.size() - 1);

        StreamLakeSegmentService svc = pt.getStreamLakeSegmentService();
        assertNotNull(svc, "segment service should be created for a client-columnar topic");

        // The async consumer on the system topic drains the build requests and segments each closed
        // ledger; give it a generous window (subscribe + consume + build).
        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            for (long closed : closedLedgers) {
                StreamLakeCatalog.LedgerInfo info = svc.catalog().get(closed);
                assertNotNull(info, "closed ledger " + closed + " should be in the catalog");
                assertTrue(info.state == StreamLakeCatalog.State.SEGMENTED && info.hasSegment(),
                        "closed ledger " + closed + " should be segmented via the system-topic consumer");
            }
        });

        // Prove the build actually went through the (sharded) system topic: aggregate stats across its
        // partitions must show at least one request per closed ledger.
        long published = admin.topics().getPartitionedStats(systemTopic, false).getMsgInCounter();
        assertTrue(published >= closedLedgers.size(),
                "sharded system topic should carry one build request per closed ledger, was " + published
                        + " for " + closedLedgers.size() + " closed ledgers");
    }
}
