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
import static org.testng.Assert.assertTrue;
import java.util.HashMap;
import java.util.List;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link StreamLakeCatalog}: per-data-ledger records upsert with latest-wins, drive
 * date pruning ({@code candidateLedgers}) and the segment-build queue ({@code closedUnsegmented}),
 * and survive a reopen by replaying the durable catalog ledger, against a {@link PulsarMockBookKeeper}.
 */
public class StreamLakeCatalogTest {

    private static final String LEDGER_NAME = "tenant/ns/persistent/sl-catalog";
    private static final long DAY1 = 1_700_000_000_000L;
    private static final long DAY2 = DAY1 + 24L * 3600 * 1000;

    private OrderedExecutor executor;
    private PulsarMockBookKeeper bk;
    private MetadataStore store;
    private ManagedLedger ml;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("sl-catalog-test").build();
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

    private static StreamLakeCatalog.LedgerInfo info(long id, long minEt, long maxEt, long rows,
            StreamLakeCatalog.State state) {
        return new StreamLakeCatalog.LedgerInfo(id, DAY1, minEt, maxEt, rows, state);
    }

    @Test
    public void upsertGetAndPruning() throws Exception {
        StreamLakeCatalog cat = StreamLakeCatalog.open(bk, ml, metaStore());
        cat.upsert(info(100L, DAY1, DAY1 + 3600_000, 1000, StreamLakeCatalog.State.CLOSED));
        cat.upsert(info(200L, DAY2, DAY2 + 3600_000, 2000, StreamLakeCatalog.State.OPEN));

        assertEquals(cat.get(100L).rowCount, 1000);
        assertEquals(cat.get(100L).state, StreamLakeCatalog.State.CLOSED);

        // date pruning: only ledger 100 covers DAY1
        List<Long> day1 = cat.candidateLedgers(DAY1, DAY1 + 1000);
        assertEquals(day1, java.util.Arrays.asList(100L));
        // both ledgers intersect a wide range
        assertEquals(cat.candidateLedgers(DAY1, DAY2 + 3600_000).size(), 2);

        // streaming variant yields the same candidate ledgers as the List variant (holds no buffer)
        List<Long> streamedDay1 = new java.util.ArrayList<>();
        cat.forEachCandidateLedger(DAY1, DAY1 + 1000, streamedDay1::add);
        assertEquals(streamedDay1, day1);
        List<Long> streamedWide = new java.util.ArrayList<>();
        cat.forEachCandidateLedger(DAY1, DAY2 + 3600_000, streamedWide::add);
        assertEquals(streamedWide, cat.candidateLedgers(DAY1, DAY2 + 3600_000));

        // segment-build queue: only the CLOSED ledger
        assertEquals(cat.closedUnsegmented(), java.util.Arrays.asList(100L));
        cat.close();
    }

    @Test
    public void stateTransitionsLatestWins() throws Exception {
        StreamLakeCatalog cat = StreamLakeCatalog.open(bk, ml, metaStore());
        cat.upsert(info(100L, DAY1, DAY2, 500, StreamLakeCatalog.State.OPEN));
        cat.markState(100L, StreamLakeCatalog.State.CLOSED);
        assertEquals(cat.closedUnsegmented(), java.util.Arrays.asList(100L));

        cat.markState(100L, StreamLakeCatalog.State.SEGMENTED);
        assertTrue(cat.closedUnsegmented().isEmpty(), "segmented ledger leaves the build queue");
        assertEquals(cat.get(100L).state, StreamLakeCatalog.State.SEGMENTED);
        assertEquals(cat.get(100L).rowCount, 500, "other fields preserved across a state transition");
        cat.close();
    }

    @Test
    public void survivesReopenByReplayingCatalogLedger() throws Exception {
        StreamLakeCatalog cat = StreamLakeCatalog.open(bk, ml, metaStore());
        cat.upsert(info(100L, DAY1, DAY2, 1000, StreamLakeCatalog.State.CLOSED));
        cat.upsert(info(200L, DAY2, DAY2 + 1000, 2000, StreamLakeCatalog.State.CLOSED));
        cat.markState(100L, StreamLakeCatalog.State.SEGMENTED);
        cat.close(); // like a topic unload

        StreamLakeCatalog reopened = StreamLakeCatalog.open(bk, ml, metaStore());
        assertEquals(reopened.get(100L).state, StreamLakeCatalog.State.SEGMENTED, "latest state replayed");
        assertEquals(reopened.get(100L).rowCount, 1000);
        assertEquals(reopened.get(200L).state, StreamLakeCatalog.State.CLOSED);
        // only ledger 200 is still closed-unsegmented after reload
        assertEquals(reopened.closedUnsegmented(), java.util.Arrays.asList(200L));
        assertFalse(reopened.all().isEmpty());
        reopened.close();
    }
}
