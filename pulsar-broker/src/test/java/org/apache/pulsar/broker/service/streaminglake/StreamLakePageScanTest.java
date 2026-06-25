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
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Slice 3C: a StreamLake predicate scan over the live batched topic -- bookie PAGE_PRUNE picks
 * candidate pages, the broker decodes only those and keeps the matching rows (design points
 * 10-13). Verified on a real broker + real bookie against a brute-force oracle.
 */
public class StreamLakePageScanTest extends StreamLakeRealBookieTestBase {

    private static final short DEPT = 1;
    private static final int N = 100;

    @Test(timeOut = 180_000)
    public void predicateScanOverLiveBatchedTopic() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sl-scan";
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
            int dept = i < 50 ? 5 : 15;
            sends.add(producer.newMessage()
                    .property("departmentId", String.valueOf(dept))
                    .value(("person-" + i).getBytes())
                    .sendAsync());
        }
        for (CompletableFuture<MessageId> f : sends) {
            f.get(60, TimeUnit.SECONDS);
        }

        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        BookKeeper bk = new BookKeeper(bkConf);
        try {
            // departmentId > 10
            List<byte[]> matched = StreamLakePageScan.scan(pt, bk,
                    StreamLakePageScan.gt(DEPT, "departmentId", 10));

            Set<String> got = new HashSet<>();
            for (byte[] v : matched) {
                got.add(new String(v));
            }
            Set<String> expected = new HashSet<>();
            for (int i = 50; i < N; i++) {
                expected.add("person-" + i);
            }
            assertEquals(got, expected, "scan must return exactly the departmentId>10 rows");
        } finally {
            bk.close();
        }
        producer.close();
    }
}
