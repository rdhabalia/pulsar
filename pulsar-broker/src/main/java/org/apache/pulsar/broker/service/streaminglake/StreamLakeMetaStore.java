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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable pointers for a topic's StreamLake metadata ledgers (the per-data-ledger catalog, the
 * segment index-ledger chain, and the shared page-index ledger chain), kept in a <b>dedicated
 * metadata node</b> that mirrors the managed-ledger path under a separate {@code /streamlake} root —
 * e.g. {@code /streamlake/tenant/ns/persistent/topic}.
 *
 * <p>This node is the single durable source of truth for "which StreamLake ledgers exist". It
 * intentionally has <b>zero</b> coupling to managed-ledger metadata — it never reads or writes the
 * managed-ledger znode/properties. Updates use optimistic concurrency against this node only, so they
 * never contend with the ledger's own write-path metadata (ledger rollover). The node's identity is
 * derived from {@code ml.getName()} (just the topic path), and its value is:
 * <pre>version(1) | flags(1) | nSeg(4) | segIds(8*n) | nPi(4) | piIds(8*n) | catalogId(8)</pre>
 */
public class StreamLakeMetaStore {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeMetaStore.class);
    private static final String ROOT = "/streamlake";
    private static final byte VERSION = 5; // version(1) flags(1) nSeg(4) segIds nPi(4) piIds catalogId(8)
    private static final int FLAG_CATALOG = 0x1;
    private static final long OP_TIMEOUT_SEC = 30;
    private static final int MAX_ATTEMPTS = 5;

    private final MetadataStore store;
    private final String name;
    private final String path;

    public StreamLakeMetaStore(MetadataStore store, ManagedLedger ml) {
        this(store, ml.getName());
    }

    /** Open the metastore by managed-ledger name (topic path) alone -- for headless/off-broker builds. */
    public StreamLakeMetaStore(MetadataStore store, String managedLedgerName) {
        this.store = store;
        this.name = managedLedgerName;
        this.path = ROOT + "/" + managedLedgerName;
    }

    /** The managed-ledger name (topic path) this metastore node is keyed by. */
    public String name() {
        return name;
    }

    /** The persisted index pointers; a null/empty field means "not set". */
    public static final class Record {
        /** The chain of segment index-ledgers (append to the head; new head on fence; roll for GC). */
        public List<Long> segmentLedgerIds = new ArrayList<>();
        /** The chain of shared page-index ledgers (per-batch stat footers; new head on roll/fence). */
        public List<Long> pageIndexLedgerIds = new ArrayList<>();
        /** The per-data-ledger catalog ledger (state + timestamps + rowcount); null means "not set". */
        public Long catalogLedgerId;
    }

    /** Read the current record, or an empty one when the {@code /streamlake} node does not exist yet. */
    public synchronized Record read() throws MetadataStoreException {
        try {
            Optional<GetResult> res = store.get(path).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            if (isPresent(res)) {
                return decode(res.get().getValue());
            }
        } catch (Exception e) {
            throw asMetadataStoreException(e);
        }
        return new Record();
    }

    /** Replace the segment index-ledger chain (used when rolling a new head or collapsing on GC). */
    public synchronized void setSegmentLedgerIds(List<Long> ids) throws MetadataStoreException {
        update(rec -> rec.segmentLedgerIds = new ArrayList<>(ids));
    }

    /** Replace the shared page-index-ledger chain (used when rolling a new head or collapsing on GC). */
    public synchronized void setPageIndexLedgerIds(List<Long> ids) throws MetadataStoreException {
        update(rec -> rec.pageIndexLedgerIds = new ArrayList<>(ids));
    }

    /** Point at the per-data-ledger catalog ledger (state + timestamps + rowcount). */
    public synchronized void updateCatalogLedgerId(long id) throws MetadataStoreException {
        update(rec -> rec.catalogLedgerId = id);
    }

    /** Remove the node (topic/managed-ledger deletion); tolerates an already-absent node. */
    public synchronized void delete() throws MetadataStoreException {
        try {
            store.delete(path, Optional.empty()).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (!isNotFound(e)) {
                throw asMetadataStoreException(e);
            }
        }
    }

    /** Read-modify-write with optimistic concurrency against our own node; retry on version conflict. */
    private void update(Consumer<Record> mutator) throws MetadataStoreException {
        MetadataStoreException last = null;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            try {
                Optional<GetResult> res = store.get(path).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                Record rec;
                long expectedVersion;
                if (isPresent(res)) {
                    rec = decode(res.get().getValue());
                    expectedVersion = res.get().getStat().getVersion();
                } else {
                    rec = new Record();
                    expectedVersion = -1L; // create-if-absent
                }
                mutator.accept(rec);
                store.put(path, encode(rec), Optional.of(expectedVersion)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                return;
            } catch (Exception e) {
                if (isBadVersion(e)) {
                    last = asMetadataStoreException(e);
                    continue; // a concurrent writer won; re-read and retry
                }
                throw asMetadataStoreException(e);
            }
        }
        log.warn("StreamLake metadata update exhausted retries for {}", path);
        throw last != null ? last : new MetadataStoreException("StreamLake metadata update failed: " + path);
    }

    private static boolean isPresent(Optional<GetResult> res) {
        return res.isPresent() && res.get().getValue() != null && res.get().getValue().length >= 2;
    }

    private static byte[] encode(Record rec) {
        List<Long> segs = rec.segmentLedgerIds == null ? java.util.Collections.emptyList() : rec.segmentLedgerIds;
        List<Long> pis = rec.pageIndexLedgerIds == null ? java.util.Collections.emptyList() : rec.pageIndexLedgerIds;
        ByteBuffer bb = ByteBuffer.allocate(1 + 1 + 4 + segs.size() * 8 + 4 + pis.size() * 8 + 8);
        bb.put(VERSION);
        bb.put((byte) (rec.catalogLedgerId != null ? FLAG_CATALOG : 0));
        bb.putInt(segs.size());
        for (long id : segs) {
            bb.putLong(id);
        }
        bb.putInt(pis.size());
        for (long id : pis) {
            bb.putLong(id);
        }
        bb.putLong(rec.catalogLedgerId != null ? rec.catalogLedgerId : 0L);
        return bb.array();
    }

    private static Record decode(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.get(); // version (reserved for forward evolution)
        int flags = bb.get() & 0xFF;
        Record rec = new Record();
        if (bb.remaining() >= 4) {
            int nSeg = bb.getInt();
            for (int i = 0; i < nSeg && bb.remaining() >= 8; i++) {
                rec.segmentLedgerIds.add(bb.getLong());
            }
        }
        if (bb.remaining() >= 4) {
            int nPi = bb.getInt();
            for (int i = 0; i < nPi && bb.remaining() >= 8; i++) {
                rec.pageIndexLedgerIds.add(bb.getLong());
            }
        }
        if (bb.remaining() >= 8) {
            long catId = bb.getLong();
            if ((flags & FLAG_CATALOG) != 0) {
                rec.catalogLedgerId = catId;
            }
        }
        return rec;
    }

    private static boolean isBadVersion(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof MetadataStoreException.BadVersionException) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNotFound(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof MetadataStoreException.NotFoundException) {
                return true;
            }
        }
        return false;
    }

    private static MetadataStoreException asMetadataStoreException(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof MetadataStoreException) {
                return (MetadataStoreException) t;
            }
        }
        return new MetadataStoreException(e);
    }
}
