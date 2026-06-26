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
 * SELECT o.orderId, c.region
 *   FROM Orders o JOIN Customers c ON o.customerId = c.customerId
 *  WHERE c.region = 'US-WEST' AND c.tier = 'gold'   (filters on B)
 *    AND o.day IN [8, 9]                            (filter on A; "last 2 days")
 *
 * Broker-side broadcast hash join with runtime semi-join push-down (min/max range + key-set bloom).
 * Verified on a real broker + real bookie against a brute-force oracle, plus an assertion that the
 * bloom key-set prunes Orders pages well beyond the range alone.
 */
public class StreamLakeJoinTest extends StreamLakeRealBookieTestBase {

    private static final short CUST = 1;       // customerId column id (both topics)
    private static final short DAY = 2;        // day column id (orders)
    private static final short REGION = 2;     // regionCode column id (customers)
    private static final short TIER = 3;       // tierCode column id (customers)

    private static final int CUSTOMERS = 200;
    private static final int DAYS = 10;
    private static final int GOLD_STEP = 60;   // gold US-WEST customers: customerId % 60 == 0
    private static final int US_WEST = 10;
    private static final int GOLD = 1;

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        config.setManagedLedgerMaxEntriesPerLedger(100_000); // one ledger per topic
    }

    private static boolean isGoldUsWest(int customerId) {
        return customerId % GOLD_STEP == 0;
    }

    @Test(timeOut = 240_000)
    public void ordersJoinCustomers() throws Exception {
        String orders = "persistent://" + NAMESPACE + "/orders";
        String customers = "persistent://" + NAMESPACE + "/customers";
        admin.topics().createNonPartitionedTopic(orders);
        admin.topics().createNonPartitionedTopic(customers);

        // Orders: indexed customerId + day; pages are customerId-banded (published in customer order).
        setPolicy(orders, StreamingLakeConfig.builder()
                .enabled(true).batchingEnabled(true).maxPageMessages(50).pageGroupingDelayMs(5_000)
                .indexedColumns(Arrays.asList(
                        col(CUST, "customerId"), col(DAY, "day")))
                .build());
        // Customers: indexed customerId + regionCode + tierCode.
        setPolicy(customers, StreamingLakeConfig.builder()
                .enabled(true).batchingEnabled(true).maxPageMessages(50).pageGroupingDelayMs(5_000)
                .indexedColumns(Arrays.asList(
                        col(CUST, "customerId"), col(REGION, "regionCode"), col(TIER, "tierCode")))
                .build());

        PersistentTopic ordersTopic = topic(orders);
        PersistentTopic customersTopic = topic(customers);
        Awaitility.await().untilAsserted(() -> {
            assertTrue(ordersTopic.isStreamLakeBatched());
            assertTrue(customersTopic.isStreamLakeBatched());
        });

        // produce customers (dimension): customerId 0..199, gold US-WEST iff customerId % 60 == 0
        Producer<byte[]> cp = pulsarClient.newProducer().topic(customers).enableBatching(false).create();
        List<CompletableFuture<MessageId>> cs = new ArrayList<>();
        for (int c = 0; c < CUSTOMERS; c++) {
            boolean gold = isGoldUsWest(c);
            int regionCode = gold ? US_WEST : 11 + (c % 3);
            int tierCode = gold ? GOLD : 2 + (c % 2);
            String region = gold ? "US-WEST" : "OTHER";
            cs.add(cp.newMessage()
                    .property("customerId", String.valueOf(c))
                    .property("regionCode", String.valueOf(regionCode))
                    .property("tierCode", String.valueOf(tierCode))
                    .property("region", region)
                    .value(("cust-" + c).getBytes())
                    .sendAsync());
        }
        for (CompletableFuture<MessageId> f : cs) {
            f.get(60, TimeUnit.SECONDS);
        }

        // produce orders (fact): for each customer, one order per day -> pages band by customerId
        Producer<byte[]> op = pulsarClient.newProducer().topic(orders).enableBatching(false).create();
        List<CompletableFuture<MessageId>> os = new ArrayList<>();
        for (int c = 0; c < CUSTOMERS; c++) {
            for (int d = 0; d < DAYS; d++) {
                os.add(op.newMessage()
                        .property("customerId", String.valueOf(c))
                        .property("day", String.valueOf(d))
                        .value(("order-" + c + "-" + d).getBytes())
                        .sendAsync());
            }
        }
        for (CompletableFuture<MessageId> f : os) {
            f.get(60, TimeUnit.SECONDS);
        }

        BookKeeper bk = dedicatedBkClient();
        try {
            // probe-side filter: day IN [8,9]
            List<StreamLakePageScan.Bound> ordersFilter = Arrays.asList(
                    new StreamLakePageScan.Bound(DAY, "day", 7, 10));
            // build-side filter: regionCode == 10 AND tierCode == 1
            List<StreamLakePageScan.Bound> customersFilter = Arrays.asList(
                    new StreamLakePageScan.Bound(REGION, "regionCode", US_WEST - 1, US_WEST + 1),
                    new StreamLakePageScan.Bound(TIER, "tierCode", GOLD - 1, GOLD + 1));

            StreamLakeJoin.Side a = new StreamLakeJoin.Side(ordersTopic, CUST, "customerId",
                    ordersFilter, Long.MIN_VALUE, Long.MAX_VALUE);
            StreamLakeJoin.Side b = new StreamLakeJoin.Side(customersTopic, CUST, "customerId",
                    customersFilter, Long.MIN_VALUE, Long.MAX_VALUE);

            StreamLakeJoin.JoinResult res = StreamLakeJoin.innerJoin(a, b, bk);

            // ----- correctness vs brute-force oracle -----
            Set<String> got = new HashSet<>();
            for (StreamLakeJoin.JoinRow r : res.rows) {
                assertEquals(r.right.get("region"), "US-WEST", "joined customer must be US-WEST");
                got.add(new String(r.leftValue) + "|" + r.right.get("region"));
            }
            Set<String> expected = new HashSet<>();
            for (int c = 0; c < CUSTOMERS; c++) {
                if (!isGoldUsWest(c)) {
                    continue;
                }
                for (int d = 8; d <= 9; d++) {
                    expected.add("order-" + c + "-" + d + "|US-WEST");
                }
            }
            assertEquals(got, expected, "join result must equal the brute-force oracle");
            assertEquals(res.buildRows, CUSTOMERS / GOLD_STEP + 1, "gold US-WEST customers");

            // ----- the runtime semi-join filter must prune Orders pages -----
            // (a) day filter only; (b) day + customerId range; (c) day + range + key-set (the join).
            int totalPages = StreamLakePageScan.scanRows(ordersTopic, bk, ordersFilter,
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE).pagesRead;

            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            List<byte[]> keys = new ArrayList<>();
            for (int c = 0; c < CUSTOMERS; c += GOLD_STEP) {
                min = Math.min(min, c);
                max = Math.max(max, c);
                keys.add(StreamLakePageScan.encodeKey(c));
            }
            List<StreamLakePageScan.Bound> withRange = new ArrayList<>(ordersFilter);
            withRange.add(new StreamLakePageScan.Bound(CUST, "customerId", min - 1, max + 1));
            int rangePages = StreamLakePageScan.scanRows(ordersTopic, bk, withRange,
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE).pagesRead;

            int bloomPages = res.probePagesRead; // join used range + key-set

            assertTrue(bloomPages < rangePages,
                    "bloom key-set must prune beyond range: bloom=" + bloomPages + " range=" + rangePages);
            assertTrue(rangePages <= totalPages,
                    "range cannot read more than the unfiltered scan: range=" + rangePages + " all=" + totalPages);
            assertTrue(bloomPages * 3 < totalPages,
                    "semi-join should prune most pages: read=" + bloomPages + " total=" + totalPages);
        } finally {
            bk.close();
        }
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

    private BookKeeper dedicatedBkClient() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        return new BookKeeper(conf);
    }
}
