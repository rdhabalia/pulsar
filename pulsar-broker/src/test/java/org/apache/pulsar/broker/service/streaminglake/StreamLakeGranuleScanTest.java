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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Stage A granule pruning: a page is carved into granules (zone maps of min/max + bloom), and a
 * selective scan reads only the granules whose zone map can match — skipping the others' column data
 * and per-row work. One page of 100 rows (value = i) with granuleSize 10 = 10 granules; the predicate
 * v &gt; 75 must read only the 3 granules covering [70..99].
 */
public class StreamLakeGranuleScanTest extends StreamLakeRealBookieTestBase {

    private static final short V = 1;

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        config.setManagedLedgerMaxEntriesPerLedger(100_000); // one ledger
    }

    @Test(timeOut = 120_000)
    public void granuleZoneMapsSkipGranules() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/granule";
        admin.topics().createNonPartitionedTopic(topic);

        StreamingLakeConfig cfg = StreamingLakeConfig.builder()
                .enabled(true).batchingEnabled(true)
                .maxPageMessages(100).pageGroupingDelayMs(5_000).granuleSize(10)
                .indexedColumns(Arrays.asList(StreamingLakeConfig.IndexedColumn.builder()
                        .columnId(V).name("v").type("INT").build()))
                .build();
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();

        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(pt.isStreamLakeBatched()));

        // value clustered with row order: row i has v = i, so granule g = rows [10g..10g+9] = v [10g..10g+9]
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            sends.add(producer.newMessage().property("v", String.valueOf(i))
                    .value(("v-" + i).getBytes()).sendAsync());
        }
        for (CompletableFuture<MessageId> f : sends) {
            f.get(60, TimeUnit.SECONDS);
        }

        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        BookKeeper bk = new BookKeeper(bkConf);
        try {
            // v > 75  -> granules covering [70..79], [80..89], [90..99] survive; the first 7 are skipped
            StreamLakePageScan.RowResult r = StreamLakePageScan.scanRows(pt, bk,
                    Arrays.asList(new StreamLakePageScan.Bound(V, "v", 75, null)),
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE);

            Set<String> got = new HashSet<>();
            for (StreamLakePageScan.Row row : r.rows) {
                got.add(new String(row.value));
            }
            Set<String> expected = new HashSet<>();
            for (int i = 76; i < 100; i++) {
                expected.add("v-" + i);
            }
            assertEquals(got, expected, "scan must return exactly v in (75, 99]");

            assertEquals(r.granulesTotal, 10, "100 rows / granuleSize 10 = 10 granules");
            assertEquals(r.granulesRead, 3, "only the 3 granules covering [70..99] survive v>75");
            assertTrue(r.granulesRead < r.granulesTotal, "granule pruning must skip granules");
        } finally {
            bk.close();
        }
        producer.close();
    }
}
