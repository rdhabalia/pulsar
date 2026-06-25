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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Slice 2b: a standard Pulsar producer on a StreamLake topic drives the broker to ship
 * per-message column ranges into the bookie page index (via the publish path), while normal
 * pub-sub still works. Verified on a real broker + real bookie.
 */
public class StreamLakePublishRangesTest extends StreamLakeRealBookieTestBase {

    private static final short DEPT = 1;

    private static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    @Test(timeOut = 180_000)
    public void standardProducerShipsRangesAndPubSubIntact() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sl-publish";
        admin.topics().createNonPartitionedTopic(topic);

        StreamingLakeConfig cfg = StreamingLakeConfig.builder()
                .enabled(true)
                .indexedColumns(Arrays.asList(
                        StreamingLakeConfig.IndexedColumn.builder()
                                .columnId(DEPT).name("departmentId").type("INT").build(),
                        StreamingLakeConfig.IndexedColumn.builder()
                                .columnId(2).name("salary").type("INT").build()))
                .build();
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();

        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(pt.isStreamLakeEnabled()));

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic).subscriptionName("s")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();

        // batching off so each message is its own entry (its own single-row page)
        int n = 100;
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        for (int i = 0; i < n; i++) {
            int dept = i < 50 ? 5 : 15;
            int salary = 40_000 + i * 100;
            producer.newMessage()
                    .property("departmentId", String.valueOf(dept))
                    .property("salary", String.valueOf(salary))
                    .value(("person-" + i).getBytes())
                    .send();
        }

        // pub-sub intact: the standard consumer still receives every message
        Set<String> received = new HashSet<>();
        for (int i = 0; i < n; i++) {
            Message<byte[]> m = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(m, "expected message " + i);
            received.add(new String(m.getValue()));
            consumer.acknowledge(m);
        }
        assertEquals(received.size(), n, "consumer must receive all messages");

        // the publish path shipped ranges to the bookie: dept>10 keeps only the 50 dept=15 entries
        ManagedLedger ml = pt.getManagedLedger();
        Position last = ml.getLastConfirmedEntry();
        long ledgerId = last.getLedgerId();

        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        BookKeeper bk = new BookKeeper(bkConf);
        try {
            BookieId bookie = bk.getLedgerManager().readLedgerMetadata(ledgerId)
                    .get().getValue().getAllEnsembles().firstEntry().getValue().get(0);
            BookieClient bookieClient = bk.getClientCtx().getBookieClient();

            Map<Short, List<PageRangeCodec.Range>> pred = new HashMap<>();
            pred.put(DEPT, Collections.singletonList(
                    new PageRangeCodec.Range(encInt(10), null, true, false)));
            List<Long> matched = bookieClient
                    .pagePrune(bookie, ledgerId, 0, last.getEntryId(), PageRangeCodec.encode(pred))
                    .get(30, TimeUnit.SECONDS);

            assertEquals(matched.size(), 50,
                    "bookie should keep only the 50 entries published with departmentId>10");
        } finally {
            bk.close();
        }

        producer.close();
        consumer.close();
    }
}
