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

import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.streaminglake.StreamLakeProducer;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeTopicSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Measures, on a real bookie, how many bytes StreamLake actually stores in each ledger tier for
 * Person-shaped data (the demo dataset), so the data-vs-overhead ratio is a number, not a guess. It
 * ingests the same client-columnar rows the demo does, waits for the broker to segment the closed data
 * ledgers, then reports the byte length of the data ledgers vs the page-index / segment / catalog
 * ledgers -- once with {@code name} indexed and once without (to isolate that column's per-page bloom).
 *
 * <p>Opt-in / sizeable: set {@code SL_TEST_ROWS} (default 200000). Prints the breakdown to stdout (also
 * captured in the test's system-out).
 */
public class StreamLakeStorageBreakdownTest extends StreamLakeRealBookieTestBase {

    @Override
    protected void customizeConfig(ServiceConfiguration config) {
        // Small data-ledger rollover so several ledgers close + segment during a modest ingest; the
        // per-page footer/segment cost is per page, so the data-vs-overhead RATIO is rollover-independent.
        config.setManagedLedgerMaxEntriesPerLedger(100);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
    }

    private static long rows() {
        String v = System.getenv("SL_TEST_ROWS");
        return v != null ? Long.parseLong(v.trim()) : 500_000;
    }

    @Test(timeOut = 600_000)
    public void breakdownNameIndexed() throws Exception {
        measure("PersonIdx", true);
    }

    @Test(timeOut = 600_000)
    public void breakdownNameNotIndexed() throws Exception {
        measure("PersonNoIdx", false);
    }

    private void measure(String tbl, boolean indexName) throws Exception {
        long rows = rows();
        int rowsPerPage = 1000;
        String topic = "persistent://" + NAMESPACE + "/" + tbl;
        admin.namespaces().setRetention(NAMESPACE, new RetentionPolicies(-1, -1));
        admin.topics().createNonPartitionedTopic(topic);

        StreamingLakeConfig cfg = StreamingLakeConfig.builder()
                .enabled(true).clientColumnarEnabled(true).setMaxCardinality(64).bloomFpp(0.01)
                .pageIndexEnsembleSize(1).pageIndexWriteQuorum(1).pageIndexAckQuorum(1)
                .segmentEnsembleSize(1).segmentWriteQuorum(1).segmentAckQuorum(1)
                .pageIndexMaxEntriesPerLedger(1_000_000).segmentMaxEntriesPerLedger(1_000_000)
                .columns(Arrays.asList(
                        new StreamingLakeConfig.SchemaColumn(1, "personId", "INT64", true),
                        new StreamingLakeConfig.SchemaColumn(2, "name", "STRING", indexName),
                        new StreamingLakeConfig.SchemaColumn(3, "age", "INT32", true)))
                .build();
        pulsar.getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(topic), false, false,
                p -> p.setStreamingLake(cfg)).get();
        Awaitility.await().untilAsserted(() -> assertTrue(topic(topic).isStreamLakeClientColumnar()));

        List<Integer> indexed = indexName ? Arrays.asList(0, 1, 2) : Arrays.asList(0, 2);
        loadPerson(topic, rows, rowsPerPage, indexed);

        PersistentTopic pt = topic(topic);
        Awaitility.await().atMost(180, TimeUnit.SECONDS).untilAsserted(() -> {
            long seg = pt.getStreamLakeSegmentService().catalog().all().values().stream()
                    .filter(i -> i.state == StreamLakeCatalog.State.SEGMENTED).count();
            assertTrue(seg >= 2, "expected segmented data ledgers, was " + seg);
        });

        long dataBytes = pt.getManagedLedger().getTotalSize();
        StreamLakeMetaStore.Record rec = new StreamLakeMetaStore(
                pulsar.getLocalMetadataStore(), pt.getManagedLedger()).read();
        long piBytes = sumLedgers(rec.pageIndexLedgerIds, "streamlake-page");
        long segBytes = sumLedgers(rec.segmentLedgerIds, "streamlake-seg");
        long catBytes = rec.catalogLedgerId != null
                ? sumLedgers(Arrays.asList(rec.catalogLedgerId), "streamlake-catalog") : 0;
        long meta = piBytes + segBytes + catBytes;
        long pages = rows / rowsPerPage;
        int dataLedgers = pt.getStreamLakeSegmentService().catalog().all().size();

        StringBuilder r = new StringBuilder();
        r.append(String.format("%n===== StreamLake storage breakdown: %s (name indexed=%s) =====%n",
                tbl, indexName));
        r.append(String.format("  rows=%,d  rowsPerPage=%d  pages=%,d  dataLedgers=%d  (ZSTD)%n",
                rows, rowsPerPage, pages, dataLedgers));
        r.append(String.format("  data-ledger : %13d B  %8s  (%.1f B/row, %.0f B/page)%n",
                dataBytes, mb(dataBytes), dataBytes / (double) rows, dataBytes / (double) pages));
        r.append(String.format("  page-index  : %13d B  %8s  = %5.1f%% of data  (%.0f B/page)%n",
                piBytes, mb(piBytes), pct(piBytes, dataBytes), piBytes / (double) pages));
        r.append(String.format("  segment     : %13d B  %8s  = %5.1f%% of data%n",
                segBytes, mb(segBytes), pct(segBytes, dataBytes)));
        r.append(String.format("  catalog     : %13d B  %8s%n", catBytes, mb(catBytes)));
        r.append(String.format("  ---- overhead (pi+seg+cat) = %s = %.1f%% of data;  total/data = %.2fx%n",
                mb(meta), pct(meta, dataBytes), (dataBytes + meta) / (double) dataBytes));
        System.out.println(r);
    }

    private long sumLedgers(List<Long> ids, String password) throws Exception {
        long total = 0;
        for (long id : ids) {
            // Recovering open (not openLedgerNoRecovery): the page-index/catalog/segment ledgers are
            // still OPEN (shared, appended-to), and an unrecovered handle reports length 0. Ingestion is
            // finished here, so fencing + recovering to read the true length is safe.
            LedgerHandle lh = pulsar.getBookKeeperClient().openLedger(
                    id, BookKeeper.DigestType.CRC32, password.getBytes());
            total += lh.getLength();
            lh.close();
        }
        return total;
    }

    private void loadPerson(String topic, long rows, int rowsPerPage, List<Integer> indexed)
            throws Exception {
        StreamLakeSchema schema = new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("personId", StreamLakeType.INT64),
                new StreamLakeSchema.Column("name", StreamLakeType.STRING),
                new StreamLakeSchema.Column("age", StreamLakeType.INT32)));
        StreamLakeTopicSchema ts = new StreamLakeTopicSchema(schema, indexed, 64, 0.01);
        Producer<byte[]> raw = pulsarClient.newProducer().topic(topic)
                .enableBatching(false).compressionType(CompressionType.NONE).create();
        try (StreamLakeProducer p = new StreamLakeProducer(raw, ts, rowsPerPage, 1 << 30, 0)) {
            for (long i = 0; i < rows; i++) {
                p.addRow(new Object[]{i, "person-" + i, 20 + (int) (i % 50)});
            }
            p.flush();
        }
        raw.close();
    }

    private PersistentTopic topic(String name) {
        return (PersistentTopic) pulsar.getBrokerService().getTopicReference(name).orElseThrow();
    }

    private static double pct(long part, long whole) {
        return whole == 0 ? 0 : 100.0 * part / whole;
    }

    private static String mb(long bytes) {
        return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    }
}
