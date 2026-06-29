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

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
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
 * Runnable join demo on an in-process standalone cluster (real broker + real bookie):
 *
 * <pre>
 *   SELECT o.orderId, c.region
 *     FROM Orders o JOIN Customers c ON o.customerId = c.customerId
 *    WHERE c.region = 'US-WEST' AND c.tier = 'gold'
 *      AND o.day IN [8, 9]              -- "last 2 days"
 * </pre>
 *
 * Writes the joined rows to {@code <out>/join-result.txt} along with the prune stats that show
 * the runtime semi-join (min/max range + key-set bloom) reading only a few Orders pages.
 */
public class StreamLakeJoinDemo extends StreamLakeRealBookieTestBase {

    private static final short CUST = 1;
    private static final short DAY = 2;
    private static final short REGION = 2;
    private static final short TIER = 3;
    private static final int CUSTOMERS = 200;
    private static final int DAYS = 10;
    private static final int GOLD_STEP = 60;
    private static final int US_WEST = 10;
    private static final int GOLD = 1;

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        config.setManagedLedgerMaxEntriesPerLedger(100_000);
    }

    @Test(timeOut = 300_000)
    public void run() throws Exception {
        String dir = System.getenv("STREAMLAKE_OUT_DIR");
        if (dir == null) {
            dir = System.getProperty("streamlake.outDir", "/tmp/streamlake-out");
        }
        Path outDir = Paths.get(dir);
        Files.createDirectories(outDir);
        Path outFile = outDir.resolve("join-result.txt");

        String orders = "persistent://" + NAMESPACE + "/orders-demo";
        String customers = "persistent://" + NAMESPACE + "/customers-demo";
        admin.topics().createNonPartitionedTopic(orders);
        admin.topics().createNonPartitionedTopic(customers);

        setPolicy(orders, StreamingLakeConfig.builder().enabled(true).batchingEnabled(true)
                .maxPageMessages(50).pageGroupingDelayMs(5_000).granuleSize(10)
                .indexedColumns(Arrays.asList(col(CUST, "customerId"), col(DAY, "day"))).build());
        setPolicy(customers, StreamingLakeConfig.builder().enabled(true).batchingEnabled(true)
                .maxPageMessages(50).pageGroupingDelayMs(5_000)
                .indexedColumns(Arrays.asList(col(CUST, "customerId"), col(REGION, "regionCode"),
                        col(TIER, "tierCode"))).build());

        PersistentTopic ordersTopic = topic(orders);
        PersistentTopic customersTopic = topic(customers);
        Awaitility.await().untilAsserted(() -> {
            org.testng.Assert.assertTrue(ordersTopic.isStreamLakeBatched());
            org.testng.Assert.assertTrue(customersTopic.isStreamLakeBatched());
        });

        // customers (dimension): gold US-WEST iff customerId % 60 == 0
        Producer<byte[]> cp = pulsarClient.newProducer().topic(customers).enableBatching(false).create();
        List<CompletableFuture<MessageId>> cs = new ArrayList<>();
        for (int c = 0; c < CUSTOMERS; c++) {
            boolean gold = c % GOLD_STEP == 0;
            cs.add(cp.newMessage()
                    .property("customerId", String.valueOf(c))
                    .property("regionCode", String.valueOf(gold ? US_WEST : 11 + (c % 3)))
                    .property("tierCode", String.valueOf(gold ? GOLD : 2 + (c % 2)))
                    .property("region", gold ? "US-WEST" : "OTHER")
                    .value(("cust-" + c).getBytes()).sendAsync());
        }
        for (CompletableFuture<MessageId> f : cs) {
            f.get(60, TimeUnit.SECONDS);
        }

        // orders (fact): one order per customer per day; pages band by customerId
        Producer<byte[]> op = pulsarClient.newProducer().topic(orders).enableBatching(false).create();
        List<CompletableFuture<MessageId>> os = new ArrayList<>();
        for (int c = 0; c < CUSTOMERS; c++) {
            for (int d = 0; d < DAYS; d++) {
                os.add(op.newMessage()
                        .property("customerId", String.valueOf(c)).property("day", String.valueOf(d))
                        .value(("order-" + c + "-" + d).getBytes()).sendAsync());
            }
        }
        for (CompletableFuture<MessageId> f : os) {
            f.get(60, TimeUnit.SECONDS);
        }

        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        BookKeeper bk = new BookKeeper(bkConf);
        StreamLakeJoin.JoinResult res;
        int totalPages;
        try {
            List<StreamLakePageScan.Bound> ordersFilter = Arrays.asList(
                    new StreamLakePageScan.Bound(DAY, "day", 7, 10));
            List<StreamLakePageScan.Bound> customersFilter = Arrays.asList(
                    new StreamLakePageScan.Bound(REGION, "regionCode", US_WEST - 1, US_WEST + 1),
                    new StreamLakePageScan.Bound(TIER, "tierCode", GOLD - 1, GOLD + 1));
            totalPages = StreamLakePageScan.scanRows(ordersTopic, bk, ordersFilter,
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE).pagesRead;
            res = StreamLakeJoin.innerJoin(
                    new StreamLakeJoin.Side(ordersTopic, CUST, "customerId", ordersFilter,
                            Long.MIN_VALUE, Long.MAX_VALUE),
                    new StreamLakeJoin.Side(customersTopic, CUST, "customerId", customersFilter,
                            Long.MIN_VALUE, Long.MAX_VALUE),
                    bk);
        } finally {
            bk.close();
        }

        List<String> lines = new ArrayList<>();
        for (StreamLakeJoin.JoinRow r : res.rows) {
            lines.add(new String(r.leftValue) + "  ->  region=" + r.right.get("region"));
        }
        lines.sort(Comparator.naturalOrder());
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(outFile, StandardCharsets.UTF_8))) {
            w.println("# Orders JOIN Customers ON customerId");
            w.println("# WHERE c.region='US-WEST' AND c.tier='gold' AND o.day IN [8,9]");
            w.println("# build side (gold US-WEST customers) = " + res.buildRows + " rows");
            w.println("# Orders pages read: " + res.probePagesRead + " of " + totalPages
                    + "  (runtime semi-join range+bloom pruning)");
            w.println("# Orders granules read: " + res.probeGranulesRead + " of " + res.probeGranulesTotal
                    + "  (in-page granule zone-map pruning)");
            w.println("# matched = " + res.rows.size() + " rows");
            for (String l : lines) {
                w.println(l);
            }
        }

        org.testng.Assert.assertEquals(res.rows.size(), (CUSTOMERS / GOLD_STEP + 1) * 2);
        System.out.println("STREAMLAKE_JOIN_OUT=" + outFile.toAbsolutePath());
        op.close();
        cp.close();
    }

    private StreamingLakeConfig.IndexedColumn col(short id, String name) {
        return StreamingLakeConfig.IndexedColumn.builder().columnId(id).name(name).type("INT").build();
    }

    private void setPolicy(String topic, StreamingLakeConfig cfg) throws Exception {
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();
    }

    private PersistentTopic topic(String name) throws Exception {
        return (PersistentTopic) pulsar.getBrokerService().getTopicReference(name).orElseThrow();
    }
}
