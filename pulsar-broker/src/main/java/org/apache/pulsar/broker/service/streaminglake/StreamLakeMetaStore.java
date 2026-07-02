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
 * Durable pointers for a topic's StreamLake indexes (the date-partition-list ledger and the segment
 * index-ledger), kept in a <b>dedicated metadata node</b> that mirrors the managed-ledger path under
 * a separate {@code /streamlake} root — e.g. {@code /streamlake/tenant/ns/persistent/topic}.
 *
 * <p>This intentionally does <b>not</b> use managed-ledger properties: writing a property rewrites
 * the managed-ledger znode and bumps its version, which would contend with the ledger's own
 * write-path metadata updates (ledger rollover). Owning a separate node means updates here use
 * optimistic concurrency only against our own node and never touch the managed-ledger metadata.
 *
 * <p>Backward compatibility: if the node does not yet exist, {@link #read()} seeds
 * {@code datePartitionLedgerId} from the legacy {@code streamlake.datePartitionLedgerId} managed-
 * ledger property (a read only — it never writes the managed-ledger znode); the value is persisted
 * into {@code /streamlake} on the next update.
 */
public class StreamLakeMetaStore {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeMetaStore.class);
    private static final String ROOT = "/streamlake";
    private static final String LEGACY_DATE_PROP = "streamlake.datePartitionLedgerId";
    private static final byte VERSION = 1;
    private static final int RECORD_SIZE = 18; // version(1) + flags(1) + dateId(8) + segId(8)
    private static final int FLAG_DATE = 0x1;
    private static final int FLAG_SEG = 0x2;
    private static final long OP_TIMEOUT_SEC = 30;
    private static final int MAX_ATTEMPTS = 5;

    private final MetadataStore store;
    private final ManagedLedger ml;
    private final String path;

    public StreamLakeMetaStore(MetadataStore store, ManagedLedger ml) {
        this.store = store;
        this.ml = ml;
        this.path = ROOT + "/" + ml.getName();
    }

    /** The persisted index pointers; a null field means "not set". */
    public static final class Record {
        public Long datePartitionLedgerId;
        public Long segmentIndexLedgerId;
    }

    /**
     * Read the current record, or an empty one seeded from the legacy managed-ledger property when
     * the {@code /streamlake} node does not exist yet. Never writes.
     */
    public synchronized Record read() throws MetadataStoreException {
        try {
            Optional<GetResult> res = store.get(path).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            if (isPresent(res)) {
                return decode(res.get().getValue());
            }
        } catch (Exception e) {
            throw asMetadataStoreException(e);
        }
        return seedFromLegacy();
    }

    public synchronized void updateDatePartitionLedgerId(long id) throws MetadataStoreException {
        update(rec -> rec.datePartitionLedgerId = id);
    }

    public synchronized void updateSegmentIndexLedgerId(long id) throws MetadataStoreException {
        update(rec -> rec.segmentIndexLedgerId = id);
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
                    rec = seedFromLegacy();
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

    private Record seedFromLegacy() {
        Record rec = new Record();
        String legacy = ml.getProperties().get(LEGACY_DATE_PROP);
        if (legacy != null) {
            try {
                rec.datePartitionLedgerId = Long.parseLong(legacy);
            } catch (NumberFormatException ignore) {
                // legacy value unparseable -> start fresh
            }
        }
        return rec;
    }

    private static boolean isPresent(Optional<GetResult> res) {
        return res.isPresent() && res.get().getValue() != null && res.get().getValue().length >= RECORD_SIZE;
    }

    private static byte[] encode(Record rec) {
        ByteBuffer bb = ByteBuffer.allocate(RECORD_SIZE);
        int flags = 0;
        if (rec.datePartitionLedgerId != null) {
            flags |= FLAG_DATE;
        }
        if (rec.segmentIndexLedgerId != null) {
            flags |= FLAG_SEG;
        }
        bb.put(VERSION);
        bb.put((byte) flags);
        bb.putLong(rec.datePartitionLedgerId != null ? rec.datePartitionLedgerId : 0L);
        bb.putLong(rec.segmentIndexLedgerId != null ? rec.segmentIndexLedgerId : 0L);
        return bb.array();
    }

    private static Record decode(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.get(); // version (reserved for future format changes)
        int flags = bb.get() & 0xFF;
        long dateId = bb.getLong();
        long segId = bb.getLong();
        Record rec = new Record();
        if ((flags & FLAG_DATE) != 0) {
            rec.datePartitionLedgerId = dateId;
        }
        if ((flags & FLAG_SEG) != 0) {
            rec.segmentIndexLedgerId = segId;
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
