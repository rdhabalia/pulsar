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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Slice 3A: a batched StreamLake topic packs many messages into few columnar page entries, the
 * producer's sends still complete (durability), and the bookie prunes whole pages by column
 * range. Verified on a real broker + real bookie.
 */
public class StreamLakeBatchedPublishTest extends StreamLakeRealBookieTestBase {

    private static final short DEPT = 1;
    private static final int N = 100;
    private static final int PAGE = 25;

    private static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    @Test(timeOut = 180_000)
    public void packsMessagesIntoColumnarPagesAndBookiePrunes() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sl-batched";
        admin.topics().createNonPartitionedTopic(topic);

        StreamingLakeConfig cfg = StreamingLakeConfig.builder()
                .enabled(true).batchingEnabled(true)
                .maxPageMessages(PAGE).pageGroupingDelayMs(5_000).pageSizeBytes(8 * 1024 * 1024)
                .indexedColumns(Arrays.asList(StreamingLakeConfig.IndexedColumn.builder()
                        .columnId(DEPT).name("departmentId").type("INT").build()))
                .build();
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();

        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(pt.isStreamLakeEnabled()));

        // batching off on the client; broker does the batching into pages
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            int dept = i < 50 ? 5 : 15; // first two pages dept=5, last two dept=15
            sends.add(producer.newMessage()
                    .property("departmentId", String.valueOf(dept))
                    .value(("person-" + i).getBytes())
                    .sendAsync());
        }
        // all sends complete only after their page is persisted (durability)
        for (CompletableFuture<MessageId> f : sends) {
            f.get(60, TimeUnit.SECONDS);
        }

        // batched: 100 messages collapsed into 4 page entries
        ManagedLedger ml = pt.getManagedLedger();
        long entries = ml.getNumberOfEntries();
        assertEquals(entries, N / PAGE, "100 messages should pack into 4 page entries");

        // bookie prunes whole pages: dept>10 keeps only the 2 pages holding dept=15
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
            assertEquals(matched.size(), 2, "bookie should keep only the 2 dept>10 pages");
        } finally {
            bk.close();
        }
        producer.close();
    }
}
