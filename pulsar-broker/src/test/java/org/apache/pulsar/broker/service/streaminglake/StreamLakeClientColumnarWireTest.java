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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.streaminglake.StreamLakeArrowBatchEncoder;
import org.apache.pulsar.client.streaminglake.StreamLakeBatchPayload;
import org.apache.pulsar.client.streaminglake.StreamLakeBatchStats;
import org.apache.pulsar.client.streaminglake.StreamLakeStatsBuilder;
import org.apache.pulsar.client.streaminglake.StreamLakeTopicSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * End-to-end wiring of the client-columnar redesign write path on a real bookie: a client-encoded
 * columnar payload (Arrow batch + stats footer) is published, the broker persists it as a normal
 * entry, and {@link org.apache.pulsar.broker.service.persistent.PersistentTopic#addComplete} slices
 * the footer from the entry tail into the topic's {@link StreamLakePageIndex}, keyed by the real
 * data-ledger entry. The stored footer decodes back to the per-column stats.
 */
public class StreamLakeClientColumnarWireTest extends StreamLakeRealBookieTestBase {

    private static StreamLakeTopicSchema topicSchema(StreamingLakeConfig cfg) {
        return StreamLakeTopicSchema.fromConfig(cfg);
    }

    @Test(timeOut = 180_000)
    public void footerIsSlicedIntoThePageIndexOnPublish() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sl-clientcolumnar-wire";
        admin.topics().createNonPartitionedTopic(topic);

        StreamingLakeConfig cfg = StreamingLakeConfig.builder()
                .enabled(true).clientColumnarEnabled(true)
                .setMaxCardinality(64).bloomFpp(0.01)
                // Single-bookie test ensemble: keep the page-index replication at 1/1/1.
                .pageIndexEnsembleSize(1).pageIndexWriteQuorum(1).pageIndexAckQuorum(1)
                .columns(Arrays.asList(
                        new StreamingLakeConfig.SchemaColumn(1, "id", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(2, "deptId", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(3, "email", "STRING", true)))
                .build();
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();

        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(pt.isStreamLakeClientColumnar()));

        // Build a client columnar payload (Arrow batch + stats footer) exactly as StreamLakeProducer would.
        StreamLakeTopicSchema schema = topicSchema(cfg);
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            rows.add(new Object[]{i, i % 4, "user-" + i + "@x.com"});
        }
        byte[] arrow;
        try (StreamLakeArrowBatchEncoder encoder = new StreamLakeArrowBatchEncoder(schema.schema())) {
            arrow = encoder.encode(rows);
        }
        byte[] footer = StreamLakeStatsBuilder.build(schema.schema(), rows, schema.indexedColumns(),
                schema.setMaxCardinality(), schema.bloomFpp()).encode();
        byte[] payload = StreamLakeBatchPayload.combine(arrow, footer);

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        MessageId id = producer.send(payload);
        long dataLedgerId = ((MessageIdImpl) id).getLedgerId();
        long dataEntryId = ((MessageIdImpl) id).getEntryId();
        producer.close();

        // addComplete offloads the footer append; wait for it to land in the page index.
        Awaitility.await().untilAsserted(() -> {
            StreamLakePageIndex pi = pt.getStreamLakePageIndex();
            assertNotNull(pi, "page index should be created for a client-columnar topic");
            assertTrue(pi.covers(dataLedgerId), "footer should be recorded for the data ledger");
        });

        StreamLakePageIndex pi = pt.getStreamLakePageIndex();
        List<StreamLakePageIndex.PageFooter> footers = pi.footersFor(dataLedgerId);
        assertFalse(footers.isEmpty());
        StreamLakePageIndex.PageFooter mine = footers.stream()
                .filter(f -> f.dataEntryId == dataEntryId).findFirst().orElseThrow();

        // The stored footer round-trips to the per-column stats the client computed.
        StreamLakeBatchStats stats = StreamLakeBatchStats.decode(mine.stats);
        assertNotNull(stats.column(0), "id is indexed");
        assertNotNull(stats.column(1), "deptId is indexed");
        assertNotNull(stats.column(2), "email is indexed");
        assertEquals(stats.column(0).distinctCount(), 20, "20 distinct ids");
        assertEquals(stats.column(1).distinctCount(), 4, "deptId has 4 distinct values (i % 4)");
    }
}
