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
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable per-data-ledger catalog for a StreamLake topic (redesign): for each data ledger it records
 * {@code createTs}, the min/max event time it covers, its row count, and its lifecycle
 * {@link State}. Two roles: the read path uses the event-time bounds for coarse date pruning
 * ({@link #candidateLedgers}); the segment builder uses the state to find closed-but-not-yet-segmented
 * ledgers ({@link #closedUnsegmented}).
 *
 * <p>Backed by a single append-only BookKeeper ledger (latest entry per data ledger wins on replay)
 * whose id lives in the {@code /streamlake} node via {@link StreamLakeMetaStore#updateCatalogLedgerId}.
 * On load the ledger is replayed and rewritten into a fresh one (self-healing if the old was fenced),
 * mirroring the durable date-index ledger. Never throws on open -- falls back to an in-memory catalog.
 */
public class StreamLakeCatalog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeCatalog.class);
    private static final byte[] PASSWORD = "streamlake-catalog".getBytes();
    private static final int ENTRY_SIZE = 8 + 8 + 8 + 8 + 8 + 1; // id, createTs, minEt, maxEt, rows, state

    /** Lifecycle of a data ledger in the StreamLake catalog. */
    public enum State {
        OPEN, CLOSED, SEGMENTED;

        static State of(int ordinal) {
            State[] v = values();
            return ordinal >= 0 && ordinal < v.length ? v[ordinal] : OPEN;
        }
    }

    /** The catalog record for one data ledger. */
    public static final class LedgerInfo {
        public final long dataLedgerId;
        public final long createTs;
        public final long minEventTime;
        public final long maxEventTime;
        public final long rowCount;
        public final State state;

        public LedgerInfo(long dataLedgerId, long createTs, long minEventTime, long maxEventTime,
                          long rowCount, State state) {
            this.dataLedgerId = dataLedgerId;
            this.createTs = createTs;
            this.minEventTime = minEventTime;
            this.maxEventTime = maxEventTime;
            this.rowCount = rowCount;
            this.state = state;
        }

        LedgerInfo withState(State newState) {
            return new LedgerInfo(dataLedgerId, createTs, minEventTime, maxEventTime, rowCount, newState);
        }
    }

    private final BookKeeper bk;
    private final ManagedLedger ml;
    private final StreamLakeMetaStore metaStore;
    private final ConcurrentMap<Long, LedgerInfo> infos = new ConcurrentHashMap<>();
    private volatile LedgerHandle writeLedger; // null => in-memory only (BK unavailable)

    private StreamLakeCatalog(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore) {
        this.bk = bk;
        this.ml = ml;
        this.metaStore = metaStore;
    }

    /** Load (replay + rotate) the catalog ledger; never throws -- falls back to an in-memory catalog. */
    public static StreamLakeCatalog open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore) {
        StreamLakeCatalog cat = new StreamLakeCatalog(bk, ml, metaStore);
        try {
            cat.loadAndRotate();
        } catch (Exception e) {
            log.warn("StreamLake catalog falling back to in-memory for {}: {}", ml.getName(), e.toString());
            cat.writeLedger = null;
        }
        return cat;
    }

    /** Upsert a data ledger's catalog record (latest wins), appending it durably. */
    public synchronized void upsert(LedgerInfo info) {
        infos.put(info.dataLedgerId, info);
        appendDurably(info);
    }

    /** Transition a data ledger's state (e.g. CLOSED, SEGMENTED), preserving the other fields. */
    public synchronized void markState(long dataLedgerId, State state) {
        LedgerInfo cur = infos.get(dataLedgerId);
        if (cur == null) {
            cur = new LedgerInfo(dataLedgerId, System.currentTimeMillis(),
                    Long.MAX_VALUE, Long.MIN_VALUE, 0, state);
        } else {
            cur = cur.withState(state);
        }
        infos.put(dataLedgerId, cur);
        appendDurably(cur);
    }

    public LedgerInfo get(long dataLedgerId) {
        return infos.get(dataLedgerId);
    }

    /** Data ledgers whose [minEventTime, maxEventTime] intersects [fromMs, toMs] (date pruning). */
    public List<Long> candidateLedgers(long fromMs, long toMs) {
        List<Long> out = new ArrayList<>();
        for (LedgerInfo i : infos.values()) {
            if (i.maxEventTime >= fromMs && i.minEventTime <= toMs) {
                out.add(i.dataLedgerId);
            }
        }
        return out;
    }

    /** Data ledgers that are closed but not yet segmented (segment-build work queue). */
    public List<Long> closedUnsegmented() {
        List<Long> out = new ArrayList<>();
        for (LedgerInfo i : infos.values()) {
            if (i.state == State.CLOSED) {
                out.add(i.dataLedgerId);
            }
        }
        return out;
    }

    public Map<Long, LedgerInfo> all() {
        return infos;
    }

    @Override
    public synchronized void close() {
        LedgerHandle lh = writeLedger;
        if (lh != null) {
            try {
                lh.close();
            } catch (Exception ignore) {
                // openLedger recovery handles an unclosed ledger on the next load
            }
        }
    }

    // ---------------------------------------------------------------- durability

    private synchronized void loadAndRotate() throws Exception {
        Long oldId = metaStore.read().catalogLedgerId;
        if (oldId != null) {
            try {
                LedgerHandle old = bk.openLedger(oldId, BookKeeper.DigestType.CRC32, PASSWORD);
                long lac = old.getLastAddConfirmed();
                if (lac >= 0) {
                    Enumeration<LedgerEntry> en = old.readEntries(0, lac);
                    while (en.hasMoreElements()) {
                        mergeEntry(en.nextElement().getEntry());
                    }
                }
                old.close();
            } catch (BKException.BKNoSuchLedgerExistsException
                    | BKException.BKNoSuchLedgerExistsOnMetadataServerException e) {
                oldId = null; // pointer dangled; start fresh
            }
        }

        LedgerHandle fresh = bk.createLedger(1, 1, BookKeeper.DigestType.CRC32, PASSWORD);
        for (LedgerInfo info : infos.values()) {
            fresh.addEntry(encode(info));
        }
        this.writeLedger = fresh;
        metaStore.updateCatalogLedgerId(fresh.getId());
        if (oldId != null && oldId != fresh.getId()) {
            try {
                bk.deleteLedger(oldId);
            } catch (Exception ignore) {
                // best-effort cleanup
            }
        }
    }

    private void appendDurably(LedgerInfo info) {
        LedgerHandle lh = writeLedger;
        if (lh == null) {
            return;
        }
        try {
            lh.addEntry(encode(info));
        } catch (Exception e) {
            // catalog ledger fenced/closed: rotate to a fresh one (replays current map) and retry once.
            try {
                loadAndRotate();
                if (writeLedger != null) {
                    writeLedger.addEntry(encode(info));
                }
            } catch (Exception ex) {
                log.warn("StreamLake catalog append failed for {}: {}", ml.getName(), ex.toString());
            }
        }
    }

    private void mergeEntry(byte[] b) {
        LedgerInfo info = decode(b);
        infos.put(info.dataLedgerId, info); // append-only: a later entry supersedes an earlier one
    }

    private static byte[] encode(LedgerInfo i) {
        ByteBuffer bb = ByteBuffer.allocate(ENTRY_SIZE);
        bb.putLong(i.dataLedgerId).putLong(i.createTs).putLong(i.minEventTime)
                .putLong(i.maxEventTime).putLong(i.rowCount).put((byte) i.state.ordinal());
        return bb.array();
    }

    private static LedgerInfo decode(byte[] b) {
        ByteBuffer bb = ByteBuffer.wrap(b);
        long id = bb.getLong();
        long createTs = bb.getLong();
        long minEt = bb.getLong();
        long maxEt = bb.getLong();
        long rows = bb.getLong();
        State state = State.of(bb.get() & 0xFF);
        return new LedgerInfo(id, createTs, minEt, maxEt, rows, state);
    }
}
