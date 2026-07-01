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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.PositionFactory;
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
 * Page column compression (next-step 1) on a real broker + real bookie. A topic with
 * {@code columnCompressionEnabled} packs its INT/LONG column blocks with the smallest lossless
 * integer codec (frame-of-reference / delta / double-delta / dictionary / raw). This verifies, end
 * to end, that compression is engaged and stays invisible to every reader:
 *
 * <ol>
 *   <li>the stored page actually carries compressed columns (FLAG_VORTEX) and is smaller than the
 *       same page stored uncompressed;</li>
 *   <li>pushed-down equality and range predicate scans still return the correct rows (the bookie
 *       →broker prune + selective decode path reads the compressed columns transparently); and</li>
 *   <li>ordinary consumers still receive messages in publish order (the transcoder never touches
 *       the compressed column blocks).</li>
 * </ol>
 *
 * <p>The exhaustive per-codec round-trip and size matrix lives in the pure-unit
 * {@code StreamLakeColumnCodecTest} and {@code StreamLakeBatchPageCompressionTest}; this test proves
 * the wiring through the live publish, scan, and consume paths.
 */
public class StreamLakeCompressionTest extends StreamLakeRealBookieTestBase {

    private static final short SEQ = 1;     // dense monotonic 0..COUNT-1 -> delta
    private static final short BUCKET = 2;  // 4 distinct values -> dictionary
    private static final int COUNT = 100;

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        config.setManagedLedgerMaxEntriesPerLedger(100_000); // one ledger
    }

    private static StreamingLakeConfig.IndexedColumn col(short id, String name) {
        return StreamingLakeConfig.IndexedColumn.builder().columnId(id).name(name).type("INT").build();
    }

    /** Produce COUNT compressible rows into one sealed compressed page; return the live topic. */
    private PersistentTopic produce(String topic) throws Exception {
        StreamingLakeConfig cfg = StreamingLakeConfig.builder().enabled(true).batchingEnabled(true)
                .maxPageMessages(COUNT).pageGroupingDelayMs(5_000).granuleSize(COUNT)
                .columnCompressionEnabled(true)
                .indexedColumns(Arrays.asList(col(SEQ, "seq"), col(BUCKET, "bucket")))
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
                    .property("seq", String.valueOf(i))         // dense monotonic
                    .property("bucket", String.valueOf(i % 4))  // 4 distinct values
                    .value(("r-" + i).getBytes()).sendAsync());
        }
        for (CompletableFuture<MessageId> f : sends) {
            f.get(60, TimeUnit.SECONDS);
        }
        producer.close();
        return pt;
    }

    /** Read the single stored page entry (raw bytes) from the topic's managed ledger. */
    private ByteBuf readStoredPage(PersistentTopic pt) throws Exception {
        ManagedLedger ml = pt.getManagedLedger();
        ManagedCursor cursor = ml.newNonDurableCursor(PositionFactory.EARLIEST);
        try {
            List<Entry> entries = cursor.readEntries(1);
            Entry e = entries.get(0);
            ByteBuf copy = e.getDataBuffer().copy(); // detach from the entry's lifecycle
            e.release();
            return copy;
        } finally {
            cursor.close();
        }
    }

    private BookKeeper bk() throws Exception {
        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        return new BookKeeper(bkConf);
    }

    private static Set<String> values(StreamLakePageScan.RowResult r) {
        Set<String> got = new HashSet<>();
        for (StreamLakePageScan.Row row : r.rows) {
            got.add(new String(row.value));
        }
        return got;
    }

    @Test(timeOut = 120_000)
    public void compressedPageStaysReadableAndOrdered() throws Exception {
        String topic = "persistent://" + NAMESPACE + "/compress";
        PersistentTopic pt = produce(topic);

        // (1) the stored page carries compressed columns, and is smaller than the same page stored
        //     uncompressed. Rebuild the identical page (same real payloads + column values) with
        //     compression off as the baseline, so the comparison isolates the column codec effect.
        ByteBuf stored = readStoredPage(pt);
        try {
            assertTrue(StreamLakeBatchPage.isPage(stored), "stored entry is a StreamLake page");
            assertTrue(StreamLakeBatchPage.hasCompressedColumns(stored),
                    "columnCompressionEnabled must set FLAG_VORTEX on the stored page");
            assertEquals(StreamLakeBatchPage.messageCount(stored), COUNT);

            int storedBytes = stored.readableBytes();
            int uncompressedBytes = reEncodeUncompressedSize(stored);
            assertTrue(storedBytes < uncompressedBytes,
                    "compressed page (" + storedBytes + "B) must be smaller than the same page stored "
                            + "uncompressed (" + uncompressedBytes + "B)");
        } finally {
            stored.release();
        }

        BookKeeper bk = bk();
        try {
            // (2) equality scan (engages the exact-set/dictionary path) reads compressed columns.
            StreamLakePageScan.RowResult eq = StreamLakePageScan.scanRows(pt, bk,
                    Arrays.asList(StreamLakePageScan.Bound.eq(BUCKET, "bucket", 2)),
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE);
            Set<String> expectedEq = new HashSet<>();
            for (int i = 0; i < COUNT; i++) {
                if (i % 4 == 2) {
                    expectedEq.add("r-" + i);
                }
            }
            assertEquals(values(eq), expectedEq, "bucket==2 rows must decode correctly from compressed columns");

            // (3) range scan (min/max + selective decode) reads compressed columns: seq in (49,60).
            StreamLakePageScan.RowResult range = StreamLakePageScan.scanRows(pt, bk,
                    Arrays.asList(new StreamLakePageScan.Bound(SEQ, "seq", 49, 60)),
                    java.util.Collections.emptyMap(), Long.MIN_VALUE, Long.MAX_VALUE);
            Set<String> expectedRange = new HashSet<>();
            for (int i = 50; i <= 59; i++) {
                expectedRange.add("r-" + i);
            }
            assertEquals(values(range), expectedRange, "seq range rows must decode correctly from compressed columns");
        } finally {
            bk.close();
        }

        // (4) consumer delivery order is still publish order on the compressed topic.
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topic)
                .subscriptionName("order").subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        try {
            List<String> received = new ArrayList<>();
            for (int i = 0; i < COUNT; i++) {
                Message<byte[]> m = consumer.receive(30, TimeUnit.SECONDS);
                org.testng.Assert.assertNotNull(m, "missing message " + i);
                received.add(new String(m.getValue()));
                consumer.acknowledge(m);
            }
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < COUNT; i++) {
                expected.add("r-" + i);
            }
            assertEquals(received, expected, "compressed pages must still deliver in publish order");
        } finally {
            consumer.close();
        }
    }

    /**
     * Re-encode {@code storedPage}'s exact messages and column values with compression off, and
     * return that page's size. Because the payloads and column values are identical, the size
     * difference from the stored (compressed) page is purely the column codec.
     */
    private static int reEncodeUncompressedSize(ByteBuf storedPage) {
        int n = StreamLakeBatchPage.messageCount(storedPage);
        List<ByteBuf> msgs = StreamLakeBatchPage.decode(storedPage); // real headersAndPayload, publish order
        try {
            int[] ids = {SEQ, BUCKET};
            byte[] types = {StreamLakeBatchPage.TYPE_INT, StreamLakeBatchPage.TYPE_INT};
            long[][] values = new long[2][];
            values[0] = StreamLakeBatchPage.readColumnRange(storedPage, SEQ, 0, n);
            values[1] = StreamLakeBatchPage.readColumnRange(storedPage, BUCKET, 0, n);
            ByteBuf raw = StreamLakeBatchPage.encode(msgs, ids, types, values, 0, 0, COUNT, 0, 64, false);
            try {
                return raw.readableBytes();
            } finally {
                raw.release();
            }
        } finally {
            msgs.forEach(ByteBuf::release);
        }
    }
}
