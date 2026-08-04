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
import java.util.List;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.streaminglake.StreamLakeArrowBatchEncoder;
import org.apache.pulsar.client.streaminglake.StreamLakeBatchPayload;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;
import org.apache.pulsar.client.streaminglake.StreamLakeStatsBuilder;
import org.apache.pulsar.client.streaminglake.StreamLakeTopicSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Proves the StreamLake read-side metadata (catalog + segments + page index) can be opened <b>headlessly
 * from BookKeeper by the topic's managed-ledger name alone</b> -- with no {@link PersistentTopic} and no
 * topic ownership. This is the foundation of the off-broker (any-broker) segment build: a broker that
 * does not own the data topic reconstructs the metadata stack purely from the durable ZK metastore
 * pointer + BookKeeper ledgers, and prunes exactly the same as the owning broker.
 */
public class StreamLakeHeadlessBuildTest extends StreamLakeRealBookieTestBase {

    private static final int ROWS_PER_PAGE = 20;
    private static final int PAGES = 13; // ledgers of 4,4,4,1 -> first three close and segment

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        config.setManagedLedgerMaxEntriesPerLedger(4);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
    }

    private static StreamingLakeConfig streamLakeConfig() {
        return StreamingLakeConfig.builder()
                .enabled(true).clientColumnarEnabled(true).setMaxCardinality(64).bloomFpp(0.01)
                .pageIndexEnsembleSize(1).pageIndexWriteQuorum(1).pageIndexAckQuorum(1)
                .segmentEnsembleSize(1).segmentWriteQuorum(1).segmentAckQuorum(1)
                .pageIndexMaxEntriesPerLedger(500).segmentMaxEntriesPerLedger(50)
                .columns(Arrays.asList(
                        new StreamingLakeConfig.SchemaColumn(1, "id", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(2, "deptId", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(3, "email", "STRING", true)))
                .build();
    }

    @Test(timeOut = 180_000)
    public void headlessOpenFromBookKeeperPrunesLikeOwner() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sl-headless";
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
        long[] pageEntry = new long[PAGES];
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
                var id = producer.send(StreamLakeBatchPayload.combine(arrow, footer));
                pageLedger[p] = ((org.apache.pulsar.client.impl.MessageIdImpl) id).getLedgerId();
                pageEntry[p] = ((org.apache.pulsar.client.impl.MessageIdImpl) id).getEntryId();
            }
        }
        producer.close();

        // Wait until the owning broker has segmented the closed ledgers (so the durable metadata exists).
        StreamLakeSegmentService owner = pt.getStreamLakeSegmentService();
        Awaitility.await().untilAsserted(() -> assertTrue(owner.catalog().all().values().stream()
                .filter(i -> i.state == StreamLakeCatalog.State.SEGMENTED).count() >= 2));
        long ownerSegmented = owner.catalog().all().values().stream()
                .filter(i -> i.state == StreamLakeCatalog.State.SEGMENTED).count();

        // ---- HEADLESS open: only the managed-ledger NAME + BookKeeper + the ZK metastore node. No topic.
        String mlName = TopicName.get(topic).getPersistenceNamingEncoding();
        BookKeeper bk = pulsar.getBookKeeperClient();
        StreamLakeMetaStore metaStore = new StreamLakeMetaStore(pulsar.getLocalMetadataStore(), mlName);
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, metaStore, 1L << 30, 500, 1, 1, 1);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, metaStore, 1L << 30, 50, 1, 1, 1, 512);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, metaStore);

        // The headless catalog sees the same segmented ledgers the owner built.
        long headlessSegmented = catalog.all().values().stream()
                .filter(i -> i.state == StreamLakeCatalog.State.SEGMENTED).count();
        assertEquals(headlessSegmented, ownerSegmented,
                "headless catalog (from BK) must see the same segmented ledgers as the owner");

        // A headless pruner (reads segment + page-index from BK) prunes exactly like the owner would.
        StreamLakePruner pruner = new StreamLakePruner(catalog, segStore, pageIndex);
        StreamLakeScanPredicate matchAll = StreamLakeScanPredicate.builder()
                .range(0, StreamLakeType.INT32, 0, true, PAGES * ROWS_PER_PAGE, true).build();
        assertEquals(pruner.prune(0, Long.MAX_VALUE, matchAll).size(), PAGES,
                "headless match-all reaches every page via the catalog");

        StreamLakeScanPredicate selective = StreamLakeScanPredicate.builder()
                .range(0, StreamLakeType.INT32, 45, true, 55, true).build();
        List<StreamLakePruner.PagePointer> pruned = pruner.prune(0, Long.MAX_VALUE, selective);
        assertEquals(pruned.size(), 1, "headless selective predicate prunes to a single page");
        assertEquals(pruned.get(0).ledgerId, pageLedger[2], "surviving page is page 2's data ledger");
        assertEquals(pruned.get(0).entryId, pageEntry[2], "surviving page is page 2's data entry");
        System.out.printf("%nHEADLESS open from BK: %d segmented ledgers, selective prune -> 1 page%n",
                headlessSegmented);
    }
}
