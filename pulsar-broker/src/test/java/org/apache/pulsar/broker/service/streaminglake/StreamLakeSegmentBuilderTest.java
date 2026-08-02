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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.client.streaminglake.StreamLakeColumnSegment;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeStatsBuilder;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * End-to-end segment build (redesign): per-batch footers land in the page index, the builder merges
 * every {@code pagesPerSegment} pages into a segment stored in the segment store, and the catalog is
 * marked SEGMENTED. Verified against an in-memory {@link PulsarMockBookKeeper}.
 */
public class StreamLakeSegmentBuilderTest {

    private static final String LEDGER_NAME = "tenant/ns/persistent/sl-segbuild";
    private static final long DATA_LEDGER = 100L;

    private OrderedExecutor executor;
    private PulsarMockBookKeeper bk;
    private MetadataStore store;
    private ManagedLedger ml;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("sl-segbuild-test").build();
        bk = new PulsarMockBookKeeper(executor);
        store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());
        ml = mock(ManagedLedger.class);
        when(ml.getName()).thenReturn(LEDGER_NAME);
        when(ml.getProperties()).thenReturn(new HashMap<>());
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
        if (bk != null) {
            bk.shutdown();
            bk = null;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    private StreamLakeMetaStore metaStore() {
        return new StreamLakeMetaStore(store, ml);
    }

    private static StreamLakeSchema schema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("deptId", StreamLakeType.INT32),
                new StreamLakeSchema.Column("email", StreamLakeType.STRING)));
    }

    private static byte[] footer(int deptFrom, int deptTo, int emailBase) {
        List<Object[]> rows = new ArrayList<>();
        for (int d = deptFrom; d <= deptTo; d++) {
            rows.add(new Object[]{d, "e" + (emailBase++) + "@x.com"});
        }
        return StreamLakeStatsBuilder.build(schema(), rows, Arrays.asList(0, 1), 64, 0.01).encode();
    }

    @Test
    public void buildsSegmentsFromFootersAndMarksCatalog() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, ml, ms);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, ml, ms);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, ms);

        // 5 page footers for the data ledger, deptId ranges 0-1, 2-3, ... ; then mark it CLOSED.
        for (int i = 0; i < 5; i++) {
            pageIndex.appendFooter(DATA_LEDGER, i, footer(i * 2, i * 2 + 1, i * 10));
        }
        catalog.upsert(new StreamLakeCatalog.LedgerInfo(DATA_LEDGER, 1L, 1000L, 2000L, 10,
                StreamLakeCatalog.State.CLOSED));

        StreamLakeSegmentBuilder builder = new StreamLakeSegmentBuilder(
                pageIndex, segStore, catalog, 2L * 1024 * 1024, 0.01);
        builder.buildForLedger(DATA_LEDGER);

        // One column-oriented segment per data ledger: a 5-page directory + per-column segments.
        StreamLakeSegmentStore.LedgerSegment seg = loadSegment(catalog, segStore, DATA_LEDGER);
        assertNotNull(seg);
        assertEquals(seg.numPages(), 5);
        assertEquals(seg.pageEntryIds[0], 0L);
        assertEquals(seg.pageEntryIds[4], 4L, "position i maps to data entryId i");
        assertTrue(seg.columns.containsKey(0), "deptId column segment");
        assertTrue(seg.columns.containsKey(1), "email column segment");

        // deptId column prunes to the exact page: deptId=0 is only on page 0 (dept 0-1).
        StreamLakeColumnSegment deptSeg = seg.columns.get(0);
        boolean[] pos0 = deptSeg.candidatePositions(StreamLakeScanPredicate.builder()
                .eq(0, StreamLakeType.INT32, 0).build().columns().get(0));
        assertTrue(pos0[0], "page 0 (dept 0-1) can contain 0");
        assertFalse(pos0[1], "page 1 (dept 2-3) cannot contain 0");
        // deptId=3 is only on page 1 (dept 2-3).
        boolean[] pos3 = deptSeg.candidatePositions(StreamLakeScanPredicate.builder()
                .eq(0, StreamLakeType.INT32, 3).build().columns().get(0));
        assertTrue(pos3[1]);
        assertFalse(pos3[0]);

        // email (text) column keeps a per-page bloom: page 0's bloom contains e0@x.com.
        StreamLakeColumnSegment emailSeg = seg.columns.get(1);
        boolean[] emailPos = emailSeg.candidatePositions(StreamLakeScanPredicate.builder()
                .eq(1, StreamLakeType.STRING, "e0@x.com").build().columns().get(0));
        assertTrue(emailPos[0], "page 0 contains e0@x.com");

        // catalog transitioned to SEGMENTED and left the build queue
        assertEquals(catalog.get(DATA_LEDGER).state, StreamLakeCatalog.State.SEGMENTED);
        assertTrue(catalog.closedUnsegmented().isEmpty());
    }

    @Test
    public void buildAllClosedIsIdempotent() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        StreamLakePageIndex pageIndex = StreamLakePageIndex.open(bk, ml, ms);
        StreamLakeSegmentStore segStore = StreamLakeSegmentStore.open(bk, ml, ms);
        StreamLakeCatalog catalog = StreamLakeCatalog.open(bk, ml, ms);

        for (int i = 0; i < 3; i++) {
            pageIndex.appendFooter(DATA_LEDGER, i, footer(i, i, i * 5));
        }
        catalog.upsert(new StreamLakeCatalog.LedgerInfo(DATA_LEDGER, 1L, 1L, 2L, 3,
                StreamLakeCatalog.State.CLOSED));

        StreamLakeSegmentBuilder builder = new StreamLakeSegmentBuilder(
                pageIndex, segStore, catalog, 2L * 1024 * 1024, 0.01);
        assertEquals(builder.buildAllClosed(), Arrays.asList(DATA_LEDGER));
        assertNotNull(loadSegment(catalog, segStore, DATA_LEDGER));

        // second pass: nothing left in the queue, no duplicate segment
        assertTrue(builder.buildAllClosed().isEmpty());
        assertNotNull(loadSegment(catalog, segStore, DATA_LEDGER));
        assertEquals(loadSegment(catalog, segStore, DATA_LEDGER).numPages(), 3, "one segment, 3 pages");
    }

    /** Load a data ledger's segment on demand via its catalog offset (the new read path). */
    private static StreamLakeSegmentStore.LedgerSegment loadSegment(StreamLakeCatalog catalog,
            StreamLakeSegmentStore segStore, long dataLedgerId) throws Exception {
        StreamLakeCatalog.LedgerInfo info = catalog.get(dataLedgerId);
        return segStore.load(dataLedgerId, info.segmentLedgerId, info.segmentStartEntry,
                info.segmentEndEntry);
    }
}
