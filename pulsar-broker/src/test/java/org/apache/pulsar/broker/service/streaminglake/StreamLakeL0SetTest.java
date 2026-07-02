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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.apache.bookkeeper.bookie.storage.ldb.PageStatEntry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Phase 1b end to end on a real broker + real bookie: the broker stores a per-page exact distinct
 * set in the L0 page-range blob for low-cardinality columns (and drops it above the cap), and that
 * set is readable back through the {@code PAGE_STATS} API — which is exactly the input segment-index
 * compaction will merge into segment-level sets.
 */
public class StreamLakeL0SetTest extends StreamLakeRealBookieTestBase {

    private static final short BUCKET = 1; // low cardinality (i % 4) -> exact set kept
    private static final short SEQ = 2;    // high cardinality (i)   -> exact set dropped
    private static final int COUNT = 100;
    private static final int SET_CAP = 64;

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        config.setManagedLedgerMaxEntriesPerLedger(100_000); // one ledger
    }

    private static StreamingLakeConfig.IndexedColumn col(short id, String name) {
        return StreamingLakeConfig.IndexedColumn.builder().columnId(id).name(name).type("INT").build();
    }

    private static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    private PersistentTopic produce(String topic) throws Exception {
        StreamingLakeConfig cfg = StreamingLakeConfig.builder().enabled(true).batchingEnabled(true)
                .maxPageMessages(COUNT).pageGroupingDelayMs(5_000).granuleSize(COUNT).setMaxCardinality(SET_CAP)
                .indexedColumns(Arrays.asList(col(BUCKET, "bucket"), col(SEQ, "seq")))
                .build();
        admin.topics().createNonPartitionedTopic(topic);
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();
        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(pt.isStreamLakeBatched()));

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < COUNT; i++) {
            sends.add(producer.newMessage()
                    .property("bucket", String.valueOf(i % 4))
                    .property("seq", String.valueOf(i))
                    .value(("r-" + i).getBytes()).sendAsync());
        }
        for (CompletableFuture<MessageId> f : sends) {
            f.get(60, TimeUnit.SECONDS);
        }
        producer.close();
        return pt;
    }

    private BookKeeper bk() throws Exception {
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        return new BookKeeper(bkConf);
    }

    @Test(timeOut = 120_000)
    public void lowCardinalitySetStoredInL0AndReadableViaPageStats() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/l0set";
        PersistentTopic pt = produce(topic);
        ManagedLedger ml = pt.getManagedLedger();

        BookKeeper bk = bk();
        try {
            BookieClient bookieClient = bk.getClientCtx().getBookieClient();
            org.apache.bookkeeper.mledger.Position lac = ml.getLastConfirmedEntry();
            int pagesChecked = 0;
            for (java.util.Map.Entry<Long, LedgerInfo> e
                    : new java.util.HashMap<>(ml.getLedgersInfo()).entrySet()) {
                long ledgerId = e.getKey();
                long lastEntry = (lac != null && lac.getLedgerId() == ledgerId)
                        ? lac.getEntryId() : e.getValue().getEntries() - 1;
                if (lastEntry < 0) {
                    continue;
                }
                BookieId bookie = bk.getLedgerManager().readLedgerMetadata(ledgerId)
                        .get(30, TimeUnit.SECONDS).getValue().getAllEnsembles().firstEntry().getValue().get(0);
                List<PageStatEntry> stats = bookieClient
                        .pageStats(bookie, ledgerId, 0, lastEntry).get(30, TimeUnit.SECONDS);
                for (PageStatEntry stat : stats) {
                    PageRangeCodec.Decoded d = PageRangeCodec.decodeAll(stat.getBlob());
                    if (d.ranges.get(BUCKET) == null) {
                        continue; // not a page for our columns
                    }
                    pagesChecked++;

                    // low-cardinality column: exact set present, with exactly the 4 distinct values.
                    List<byte[]> bucketSet = d.sets.get(BUCKET);
                    assertNotNull(bucketSet, "low-cardinality bucket column must carry an exact set");
                    Set<String> got = new HashSet<>();
                    for (byte[] v : bucketSet) {
                        got.add(Arrays.toString(v));
                    }
                    Set<String> expect = new HashSet<>();
                    for (int v = 0; v < 4; v++) {
                        expect.add(Arrays.toString(encInt(v)));
                    }
                    assertEquals(got, expect, "bucket set must be exactly {0,1,2,3} encoded");

                    // high-cardinality column: distinct count (100) exceeds the cap (64) -> no set.
                    assertNull(d.sets.get(SEQ), "high-cardinality seq column must have no exact set");
                }
            }
            assertFalse(pagesChecked == 0, "expected at least one StreamLake page");
        } finally {
            bk.close();
        }
    }
}
