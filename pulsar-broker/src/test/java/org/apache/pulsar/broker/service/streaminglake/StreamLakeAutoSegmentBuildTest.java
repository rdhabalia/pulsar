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
import java.util.List;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.MessageIdImpl;
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
 * End-to-end W0 (auto read-side build) on a real bookie: producing StreamLake batches across forced
 * data-ledger rollovers makes the owning broker automatically (1) register each ledger's event-time in
 * the {@link StreamLakeCatalog} and (2) roll its page-index footers into column
 * {@link StreamLakeSegmentStore segments} when the ledger closes -- with no explicit segment-build
 * call. A {@link StreamLakePruner} built from the topic's own catalog + segment store + page index then
 * prunes correctly (a selective predicate lands on the exact page), proving the metadata a query needs
 * is now built by the live broker.
 */
public class StreamLakeAutoSegmentBuildTest extends StreamLakeRealBookieTestBase {

    private static final int ROWS_PER_PAGE = 20;
    private static final int MAX_ENTRIES_PER_LEDGER = 4;
    private static final int PAGES = 13; // -> ledgers of 4,4,4,1: the first three close and get segmented

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        // Force a data-ledger rollover every few entries so ledgers close mid-produce and W0 fires.
        config.setManagedLedgerMaxEntriesPerLedger(MAX_ENTRIES_PER_LEDGER);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
    }

    private static StreamingLakeConfig streamLakeConfig() {
        return StreamingLakeConfig.builder()
                .enabled(true).clientColumnarEnabled(true)
                .setMaxCardinality(64).bloomFpp(0.01)
                // Single-bookie ensemble: keep every StreamLake ledger's replication at 1/1/1.
                .pageIndexEnsembleSize(1).pageIndexWriteQuorum(1).pageIndexAckQuorum(1)
                .segmentEnsembleSize(1).segmentWriteQuorum(1).segmentAckQuorum(1)
                .columns(Arrays.asList(
                        new StreamingLakeConfig.SchemaColumn(1, "id", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(2, "deptId", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(3, "email", "STRING", true)))
                .build();
    }

    @Test(timeOut = 180_000)
    public void closingLedgersAutoBuildCatalogAndSegments() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sl-auto-segment";
        admin.topics().createNonPartitionedTopic(topic);

        StreamingLakeConfig cfg = streamLakeConfig();
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();

        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(pt.isStreamLakeClientColumnar()));

        // Produce PAGES columnar batches; page p carries ids [20p, 20p+19] so a range predicate is
        // selective across pages. Capture each page's (ledgerId, entryId) to know ledger boundaries.
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
                MessageId id = producer.send(StreamLakeBatchPayload.combine(arrow, footer));
                pageLedger[p] = ((MessageIdImpl) id).getLedgerId();
                pageEntry[p] = ((MessageIdImpl) id).getEntryId();
            }
        }
        producer.close();

        // The distinct data ledgers, in order; all but the last close during the run.
        List<Long> ledgersInOrder = new ArrayList<>();
        for (long lid : pageLedger) {
            if (ledgersInOrder.isEmpty() || ledgersInOrder.get(ledgersInOrder.size() - 1) != lid) {
                ledgersInOrder.add(lid);
            }
        }
        assertTrue(ledgersInOrder.size() >= 3, "expected several ledger rollovers, got " + ledgersInOrder);
        List<Long> closedLedgers = ledgersInOrder.subList(0, ledgersInOrder.size() - 1);

        // W0: the broker auto-registers + segments each closed ledger (no explicit build call).
        StreamLakeSegmentService svc = pt.getStreamLakeSegmentService();
        assertNotNull(svc, "segment service should be created for a client-columnar topic");
        Awaitility.await().untilAsserted(() -> {
            for (long closed : closedLedgers) {
                StreamLakeCatalog.LedgerInfo info = svc.catalog().get(closed);
                assertNotNull(info, "closed ledger " + closed + " should be in the catalog");
                assertEquals(info.state, StreamLakeCatalog.State.SEGMENTED,
                        "closed ledger " + closed + " should be auto-segmented");
                assertTrue(info.hasSegment(),
                        "catalog should carry the segment offset for closed ledger " + closed);
            }
        });

        // Each closed data ledger's segment loads on demand from its catalog offset with one page per
        // data entry it held.
        for (long closed : closedLedgers) {
            long pagesInLedger = Arrays.stream(pageLedger).filter(l -> l == closed).count();
            StreamLakeCatalog.LedgerInfo info = svc.catalog().get(closed);
            StreamLakeSegmentStore.LedgerSegment seg = svc.segmentStore().load(closed,
                    info.segmentLedgerId, info.segmentStartEntry, info.segmentEndEntry);
            assertNotNull(seg, "segment should load on demand for closed ledger " + closed);
            assertEquals(seg.numPages(), (int) pagesInLedger,
                    "segment page count must match the ledger's entry count");
        }

        // Build a pruner from the topic's own metadata and prune (no data read needed).
        StreamLakePruner pruner = new StreamLakePruner(svc.catalog(), svc.segmentStore(),
                pt.getStreamLakePageIndex());

        // Match-all range over id -> every page survives (all ledgers reachable via catalog).
        StreamLakeScanPredicate matchAll = StreamLakeScanPredicate.builder()
                .range(0, StreamLakeType.INT32, 0, true, PAGES * ROWS_PER_PAGE, true).build();
        List<StreamLakePruner.PagePointer> all = pruner.prune(0, Long.MAX_VALUE, matchAll);
        assertEquals(all.size(), PAGES, "match-all should reach every page via the catalog");

        // Selective range [45,55] falls only inside page 2 (ids [40,59]) -> exactly that page survives.
        StreamLakeScanPredicate selective = StreamLakeScanPredicate.builder()
                .range(0, StreamLakeType.INT32, 45, true, 55, true).build();
        List<StreamLakePruner.PagePointer> pruned = pruner.prune(0, Long.MAX_VALUE, selective);
        assertEquals(pruned.size(), 1, "selective predicate should prune to a single page");
        assertEquals(pruned.get(0).ledgerId, pageLedger[2], "surviving page is on page 2's data ledger");
        assertEquals(pruned.get(0).entryId, pageEntry[2], "surviving page is page 2's data entry");
    }
}
