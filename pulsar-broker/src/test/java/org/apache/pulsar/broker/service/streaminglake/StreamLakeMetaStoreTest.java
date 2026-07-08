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

import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link StreamLakeMetaStore}: the index pointers round-trip through the dedicated
 * {@code /streamlake/<ledgerName>} node, updates are independent, the legacy managed-ledger property
 * is seeded on first read, and — the whole point — the managed-ledger metadata is never written.
 */
public class StreamLakeMetaStoreTest {

    private static final String LEDGER_NAME = "tenant/ns/persistent/topic";

    private MetadataStore store;
    private ManagedLedger ml;
    private Map<String, String> mlProps;

    @BeforeMethod
    public void setup() throws Exception {
        store = MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        mlProps = new HashMap<>();
        ml = mock(ManagedLedger.class);
        when(ml.getName()).thenReturn(LEDGER_NAME);
        when(ml.getProperties()).thenReturn(mlProps);
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
        assertNull(rec.datePartitionLedgerId);
        assertTrue(rec.segmentLedgerIds.isEmpty());
    }

    @Test
    public void roundTripsBothPointers() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        ms.updateDatePartitionLedgerId(42L);
        ms.setSegmentLedgerIds(java.util.Arrays.asList(87L, 88L, 89L));

        StreamLakeMetaStore.Record rec = metaStore().read(); // fresh instance -> reads from the node
        assertEquals(rec.datePartitionLedgerId, Long.valueOf(42L));
        assertEquals(rec.segmentLedgerIds, java.util.Arrays.asList(87L, 88L, 89L));
    }

    @Test
    public void updatesArePreservedIndependently() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        ms.setSegmentLedgerIds(java.util.Arrays.asList(7L));
        StreamLakeMetaStore.Record afterSeg = metaStore().read();
        assertEquals(afterSeg.segmentLedgerIds, java.util.Arrays.asList(7L));
        assertNull(afterSeg.datePartitionLedgerId, "date pointer untouched by a segment update");

        ms.updateDatePartitionLedgerId(9L);
        StreamLakeMetaStore.Record both = metaStore().read();
        assertEquals(both.datePartitionLedgerId, Long.valueOf(9L));
        assertEquals(both.segmentLedgerIds, java.util.Arrays.asList(7L), "segment chain preserved");
    }

    @Test
    public void seedsDatePointerFromLegacyManagedLedgerProperty() throws Exception {
        mlProps.put("streamlake.datePartitionLedgerId", "99");

        // read (node absent) seeds the date pointer from the legacy property.
        assertEquals(metaStore().read().datePartitionLedgerId, Long.valueOf(99L));

        // a later segment update carries the seeded date pointer into the /streamlake node.
        StreamLakeMetaStore ms = metaStore();
        ms.setSegmentLedgerIds(java.util.Arrays.asList(5L));
        StreamLakeMetaStore.Record rec = metaStore().read();
        assertEquals(rec.datePartitionLedgerId, Long.valueOf(99L), "legacy date pointer migrated");
        assertEquals(rec.segmentLedgerIds, java.util.Arrays.asList(5L));
    }

    @Test
    public void deleteRemovesTheNode() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        ms.updateDatePartitionLedgerId(1L);
        assertEquals(metaStore().read().datePartitionLedgerId, Long.valueOf(1L));

        ms.delete();
        assertNull(metaStore().read().datePartitionLedgerId, "node removed -> empty again");
        ms.delete(); // idempotent: deleting an absent node is fine
    }

    @Test
    public void neverWritesManagedLedgerMetadata() throws Exception {
        StreamLakeMetaStore ms = metaStore();
        ms.updateDatePartitionLedgerId(1L);
        ms.setSegmentLedgerIds(java.util.Arrays.asList(2L));
        ms.read();
        ms.delete();

        verify(ml, never()).setProperty(any(), any());
        verify(ml, never()).setProperties(any());
        verify(ml, never()).asyncSetProperty(any(), any(), any(), any());
        assertTrue(true);
    }
}
