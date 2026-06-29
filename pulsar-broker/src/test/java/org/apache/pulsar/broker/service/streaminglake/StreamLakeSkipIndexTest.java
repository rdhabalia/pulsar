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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Three ClickHouse-style data-skipping borrows, each verified on a real broker + real bookie:
 *
 * <ol>
 *   <li><b>set(N) exact-value granule index</b> — an equality predicate prunes a granule even when
 *       the value lies inside its min/max, because the granule stores its exact distinct set.</li>
 *   <li><b>PREWHERE / late materialization</b> — the most selective predicate column is evaluated
 *       first and later columns are read only at surviving rows, shrinking {@code cellsScanned}.</li>
 *   <li><b>Sparse primary index</b> — a page sorted by its key has monotonic per-granule marks, so a
 *       predicate on that key binary-searches the candidate granule window instead of scanning all
 *       granules ({@code granulesExamined} stays tiny).</li>
 * </ol>
 */
public class StreamLakeSkipIndexTest extends StreamLakeRealBookieTestBase {

    private static final short V = 1;
    private static final short A = 1;
    private static final short B = 2;

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        config.setManagedLedgerMaxEntriesPerLedger(100_000); // one ledger
    }

    private PersistentTopic produce(String topic, StreamingLakeConfig cfg, int count,
                                    RowFiller filler) throws Exception {
        admin.topics().createNonPartitionedTopic(topic);
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();
        PersistentTopic pt = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).orElseThrow();
        Awaitility.await().untilAsserted(() -> assertTrue(pt.isStreamLakeBatched()));

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).enableBatching(false).create();
        List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            sends.add(filler.fill(producer.newMessage(), i)
                    .value(("r-" + i).getBytes()).sendAsync());
        }
        for (CompletableFuture<MessageId> f : sends) {
            f.get(60, TimeUnit.SECONDS);
        }
        producer.close();
        return pt;
    }

    private interface RowFiller {
        org.apache.pulsar.client.api.TypedMessageBuilder<byte[]> fill(
                org.apache.pulsar.client.api.TypedMessageBuilder<byte[]> m, int i);
    }

    private BookKeeper bk() throws Exception {
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        return new BookKeeper(bkConf);
    }

    private static StreamingLakeConfig.IndexedColumn col(short id, String name) {
        return StreamingLakeConfig.IndexedColumn.builder().columnId(id).name(name).type("INT").build();
    }

    // ---------------------------------------------------------------- (1) set index

    /**
     * One page, 10 granules. Granule g (rows 10g..10g+9) holds values {g, g+1000}: its min/max is
     * [g, g+1000], which <i>includes</i> 5 and 50 for every granule — so min/max alone prunes nothing
     * useful. The exact set {g, g+1000} proves the real membership: {@code v == 5} survives in exactly
     * one granule and {@code v == 50} (absent everywhere) survives in none.
     */
    @Test(timeOut = 120_000)
    public void setIndexPrunesInsideMinMax() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/setidx";
        StreamingLakeConfig cfg = StreamingLakeConfig.builder().enabled(true).batchingEnabled(true)
                .maxPageMessages(100).pageGroupingDelayMs(5_000).granuleSize(10)
                .indexedColumns(Arrays.asList(col(V, "v"))).build();
        PersistentTopic pt = produce(topic, cfg, 100, (m, i) -> {
            int g = i / 10;
            int val = (i % 2 == 0) ? g : g + 1000; // each granule's set = {g, g+1000}
            return m.property("v", String.valueOf(val));
        });

        BookKeeper bk = bk();
        try {
            // v == 5: present only as the even rows of granule 5 (value g==5). min/max keeps granules
            // 0..5; the exact set narrows it to granule 5 alone.
            StreamLakePageScan.RowResult present = StreamLakePageScan.scanRows(pt, bk,
                    Arrays.asList(StreamLakePageScan.Bound.eq(V, "v", 5)),
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE);
            Set<String> got = new HashSet<>();
            for (StreamLakePageScan.Row r : present.rows) {
                got.add(new String(r.value));
            }
            assertEquals(got, new HashSet<>(Arrays.asList("r-50", "r-52", "r-54", "r-56", "r-58")));
            assertEquals(present.granulesTotal, 10);
            assertEquals(present.granulesExamined, 10, "no sort -> every granule's zone map inspected");
            assertEquals(present.granulesRead, 1, "exact set narrows v==5 to granule 5 only");

            // v == 50: inside [g, g+1000] for every granule, but in no granule's set -> zero granules.
            StreamLakePageScan.RowResult absent = StreamLakePageScan.scanRows(pt, bk,
                    Arrays.asList(StreamLakePageScan.Bound.eq(V, "v", 50)),
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE);
            assertTrue(absent.rows.isEmpty(), "v==50 matches nothing");
            assertEquals(absent.granulesRead, 0, "exact set prunes all granules; min/max would keep 10");
        } finally {
            bk.close();
        }
    }

    // ---------------------------------------------------------------- (2) PREWHERE

    /**
     * One granule of 100 rows, two predicate columns: {@code a == 42} (matches one row) and a wide
     * range on {@code b} (matches all). PREWHERE evaluates the selective column first: it reads all
     * 100 cells of {@code a}, narrows to one survivor, then reads {@code b} for just that one row —
     * 101 cells instead of the naive 200.
     */
    @Test(timeOut = 120_000)
    public void prewhereReadsLaterColumnsOnlyForSurvivors() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/prewhere";
        StreamingLakeConfig cfg = StreamingLakeConfig.builder().enabled(true).batchingEnabled(true)
                .maxPageMessages(100).pageGroupingDelayMs(5_000).granuleSize(100)
                .indexedColumns(Arrays.asList(col(A, "a"), col(B, "b"))).build();
        PersistentTopic pt = produce(topic, cfg, 100,
                (m, i) -> m.property("a", String.valueOf(i)).property("b", String.valueOf(i)));

        BookKeeper bk = bk();
        try {
            List<StreamLakePageScan.Bound> bounds = Arrays.asList(
                    StreamLakePageScan.Bound.eq(A, "a", 42),
                    new StreamLakePageScan.Bound(B, "b", -1, 1000)); // matches every row
            StreamLakePageScan.RowResult r = StreamLakePageScan.scanRows(pt, bk, bounds,
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE);

            assertEquals(r.rows.size(), 1);
            assertEquals(new String(r.rows.get(0).value), "r-42");
            assertEquals(r.granulesRead, 1);
            assertEquals(r.cellsScanned, 101L,
                    "selective column read fully (100) + unselective column read for 1 survivor; naive = 200");
        } finally {
            bk.close();
        }
    }

    // ---------------------------------------------------------------- (3) sparse primary index

    /**
     * One page of 1000 rows, ids inserted in a scrambled permutation. With {@code sortColumnId} set,
     * the page is sorted ascending, so its 100 granules each cover a contiguous 10-id band and the
     * per-granule marks are monotonic. A predicate on the id then binary-searches a tiny granule
     * window instead of inspecting all 100 granules.
     */
    @Test(timeOut = 120_000)
    public void sparseIndexBinarySearchesSortedGranules() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sparse";
        StreamingLakeConfig cfg = StreamingLakeConfig.builder().enabled(true).batchingEnabled(true)
                .maxPageMessages(1000).pageGroupingDelayMs(5_000).granuleSize(10).sortColumnId(V)
                .indexedColumns(Arrays.asList(col(V, "id"))).build();
        // value = (i*37) % 1000 is a deterministic permutation of 0..999 (gcd(37,1000)=1)
        PersistentTopic pt = produce(topic, cfg, 1000,
                (m, i) -> m.property("id", String.valueOf((i * 37) % 1000)));

        BookKeeper bk = bk();
        try {
            // id == 512: lives in the single sorted granule [510..519].
            StreamLakePageScan.RowResult eq = StreamLakePageScan.scanRows(pt, bk,
                    Arrays.asList(StreamLakePageScan.Bound.eq(V, "id", 512)),
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE);
            assertEquals(eq.rows.size(), 1);
            assertEquals(new String(eq.rows.get(0).value), "r-" + indexOfId(512));
            assertEquals(eq.granulesTotal, 100, "1000 rows / granuleSize 10");
            assertEquals(eq.granulesExamined, 1, "binary search inspects only the one candidate granule");
            assertEquals(eq.granulesRead, 1);

            // id > 994: the suffix granule [990..999] -> ids 995..999, found by the same binary search.
            StreamLakePageScan.RowResult gt = StreamLakePageScan.scanRows(pt, bk,
                    Arrays.asList(new StreamLakePageScan.Bound(V, "id", 994, null)),
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE);
            assertEquals(gt.rows.size(), 5, "ids 995..999");
            assertEquals(gt.granulesExamined, 1, "only the last granule's window is inspected");
            assertEquals(gt.granulesRead, 1);
        } finally {
            bk.close();
        }
    }

    /**
     * The sparse index must NOT change pub-sub delivery order. With {@code sortColumnId} set and ids
     * produced in a scrambled order, an ordinary consumer must still receive messages in <b>publish
     * order</b> (r-0, r-1, …) — because the page keeps publish order on disk and only stores the sort
     * permutation as a side index the transcoder never reads.
     */
    @Test(timeOut = 120_000)
    public void sortIndexDoesNotChangeConsumerDeliveryOrder() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/sparse-order";
        StreamingLakeConfig cfg = StreamingLakeConfig.builder().enabled(true).batchingEnabled(true)
                .maxPageMessages(25).pageGroupingDelayMs(5_000).granuleSize(5).sortColumnId(V)
                .indexedColumns(Arrays.asList(col(V, "id"))).build();
        // ids scrambled so that a sorted layout would deliver a very different order than publish order
        produce(topic, cfg, 50, (m, i) -> m.property("id", String.valueOf((i * 37) % 50)));

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("order").subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        try {
            List<String> received = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                Message<byte[]> m = consumer.receive(30, TimeUnit.SECONDS);
                org.testng.Assert.assertNotNull(m, "missing message " + i);
                received.add(new String(m.getValue()));
                consumer.acknowledge(m);
            }
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                expected.add("r-" + i);
            }
            assertEquals(received, expected, "consumer must receive publish order, not sort-key order");
        } finally {
            consumer.close();
        }
    }

    /** Inverse of value = (i*37)%1000: the row index i that carries a given id. */
    private static int indexOfId(int id) {
        for (int i = 0; i < 1000; i++) {
            if ((i * 37) % 1000 == id) {
                return i;
            }
        }
        throw new IllegalStateException("no row for id " + id);
    }
}
