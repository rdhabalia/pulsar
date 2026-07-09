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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link StreamLakeMetaStore}: the ledger pointers (segment chain, page-index chain,
 * catalog pointer) round-trip through the dedicated {@code /streamlake/<ledgerName>} node, updates are
 * independent, and — the whole point — the store has <b>zero</b> coupling to managed-ledger metadata
 * (it never reads or writes the managed-ledger properties).
 */
public class StreamLakeMetaStoreTest {

    private static final String LEDGER_NAME = "tenant/ns/persistent/topic";

    private MetadataStore store;
    private ManagedLedger ml;

    @BeforeMethod
    public void setup() throws Exception {
        store = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        ml = mock(ManagedLedger.class);
        when(ml.getName()).thenReturn(LEDGER_NAME);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    private StreamLakeMetaStore metaStore() {
        return new StreamLakeMetaStore(store, ml);
    }

    @Test
    public void readsEmptyWhenAbsent() throws Exception {
        StreamLakeMetaStore.Record rec = metaStore().read();
        assertTrue(rec.segmentLedgerIds.isEmpty());
        assertTrue(rec.pageIndexLedgerIds.isEmpty());
        assertNull(rec.catalogLedgerId);
    }

    @Test
    public void roundTripsAllThreePointersIndependently() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        ms.setSegmentLedgerIds(Arrays.asList(87L, 88L));
        ms.setPageIndexLedgerIds(Arrays.asList(500L, 501L, 502L));
        ms.updateCatalogLedgerId(555L);

        StreamLakeMetaStore.Record rec = metaStore().read(); // fresh instance -> reads from the node
        assertEquals(rec.segmentLedgerIds, Arrays.asList(87L, 88L));
        assertEquals(rec.pageIndexLedgerIds, Arrays.asList(500L, 501L, 502L));
        assertEquals(rec.catalogLedgerId, Long.valueOf(555L));
    }

    @Test
    public void pageIndexChainUpdateLeavesSegmentAndCatalogUntouched() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        ms.setSegmentLedgerIds(Arrays.asList(7L));
        ms.updateCatalogLedgerId(9L);
        ms.setPageIndexLedgerIds(Arrays.asList(11L, 12L));

        StreamLakeMetaStore.Record rec = metaStore().read();
        assertEquals(rec.segmentLedgerIds, Arrays.asList(7L));
        assertEquals(rec.catalogLedgerId, Long.valueOf(9L));
        assertEquals(rec.pageIndexLedgerIds, Arrays.asList(11L, 12L));
    }

    @Test
    public void catalogPointerUpdateLeavesChainsUntouched() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        ms.setPageIndexLedgerIds(Arrays.asList(70L, 71L));
        ms.updateCatalogLedgerId(555L);

        StreamLakeMetaStore.Record rec = metaStore().read();
        assertEquals(rec.catalogLedgerId, Long.valueOf(555L));
        assertEquals(rec.pageIndexLedgerIds, Arrays.asList(70L, 71L), "chains preserved");
        assertTrue(rec.segmentLedgerIds.isEmpty());
    }

    @Test
    public void deleteRemovesTheNode() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        ms.setPageIndexLedgerIds(Arrays.asList(1L));
        assertEquals(metaStore().read().pageIndexLedgerIds, Arrays.asList(1L));

        ms.delete();
        assertTrue(metaStore().read().pageIndexLedgerIds.isEmpty(), "node removed -> empty again");
        ms.delete(); // idempotent: deleting an absent node is fine
    }

    @Test
    public void neverTouchesManagedLedgerMetadata() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        ms.setSegmentLedgerIds(Arrays.asList(2L));
        ms.setPageIndexLedgerIds(Arrays.asList(3L));
        ms.updateCatalogLedgerId(4L);
        ms.read();
        ms.delete();

        // Zero coupling: neither reads (getProperties) nor writes (setProperty/...) the ML metadata.
        verify(ml, never()).getProperties();
        verify(ml, never()).setProperty(any(), any());
        verify(ml, never()).setProperties(any());
        verify(ml, never()).asyncSetProperty(any(), any(), any(), any());
    }
}
