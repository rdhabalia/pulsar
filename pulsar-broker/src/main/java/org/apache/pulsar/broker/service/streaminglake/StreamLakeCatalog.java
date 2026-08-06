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
    // Bounded entries per readEntries() on load, so a large catalog ledger can't flood the bookie.
    private static final int LOAD_READ_BATCH = 500;
    // id, createTs, minEt, maxEt, rows, state, + segment offset (ledgerId,start,end) + page-index range.
    private static final int ENTRY_SIZE = 8 + 8 + 8 + 8 + 8 + 1 + 8 + 8 + 8 + 8 + 8 + 8;

    /** Lifecycle of a data ledger in the StreamLake catalog. */
    public enum State {
        OPEN, CLOSED, SEGMENTED;

        static State of(int ordinal) {
            State[] v = values();
            return ordinal >= 0 && ordinal < v.length ? v[ordinal] : OPEN;
        }
    }

    /** The catalog record (manifest) for one data ledger. */
    public static final class LedgerInfo {
        public final long dataLedgerId;
        public final long createTs;
        public final long minEventTime;
        public final long maxEventTime;
        public final long rowCount;
        public final State state;
        // Segment offset: where this data ledger's segment lives (a contiguous entry range in a
        // segment ledger), so a query loads exactly that segment on demand -- no chain replay. -1 = none.
        public final long segmentLedgerId;
        public final long segmentStartEntry;
        public final long segmentEndEntry;
        // Page-index range: this data ledger's footers as a contiguous entry range in a page-index
        // ledger, for on-demand exact-set/collapsed-column precision at query time. -1 = none.
        public final long pageIndexLedgerId;
        public final long pageIndexStartEntry;
        public final long pageIndexEndEntry;

        public LedgerInfo(long dataLedgerId, long createTs, long minEventTime, long maxEventTime,
                          long rowCount, State state) {
            this(dataLedgerId, createTs, minEventTime, maxEventTime, rowCount, state,
                    -1, -1, -1, -1, -1, -1);
        }

        public LedgerInfo(long dataLedgerId, long createTs, long minEventTime, long maxEventTime,
                          long rowCount, State state, long segmentLedgerId, long segmentStartEntry,
                          long segmentEndEntry, long pageIndexLedgerId, long pageIndexStartEntry,
                          long pageIndexEndEntry) {
            this.dataLedgerId = dataLedgerId;
            this.createTs = createTs;
            this.minEventTime = minEventTime;
            this.maxEventTime = maxEventTime;
            this.rowCount = rowCount;
            this.state = state;
            this.segmentLedgerId = segmentLedgerId;
            this.segmentStartEntry = segmentStartEntry;
            this.segmentEndEntry = segmentEndEntry;
            this.pageIndexLedgerId = pageIndexLedgerId;
            this.pageIndexStartEntry = pageIndexStartEntry;
            this.pageIndexEndEntry = pageIndexEndEntry;
        }

        /** True when the segment offset is set (the data ledger has been segmented). */
        public boolean hasSegment() {
            return segmentLedgerId >= 0;
        }

        LedgerInfo withState(State newState) {
            return new LedgerInfo(dataLedgerId, createTs, minEventTime, maxEventTime, rowCount, newState,
                    segmentLedgerId, segmentStartEntry, segmentEndEntry,
                    pageIndexLedgerId, pageIndexStartEntry, pageIndexEndEntry);
        }

        LedgerInfo segmented(long segLedgerId, long segStart, long segEnd,
                             long piLedgerId, long piStart, long piEnd) {
            return new LedgerInfo(dataLedgerId, createTs, minEventTime, maxEventTime, rowCount,
                    State.SEGMENTED, segLedgerId, segStart, segEnd, piLedgerId, piStart, piEnd);
        }
    }

    private final BookKeeper bk;
    private final StreamLakeMetaStore metaStore;
    private final ConcurrentMap<Long, LedgerInfo> infos = new ConcurrentHashMap<>();
    private volatile LedgerHandle writeLedger; // null => in-memory only (BK unavailable)

    private StreamLakeCatalog(BookKeeper bk, StreamLakeMetaStore metaStore) {
        this.bk = bk;
        this.metaStore = metaStore;
    }

    /** Load (replay + rotate) the catalog ledger; never throws -- falls back to an in-memory catalog. */
    public static StreamLakeCatalog open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore) {
        return open(bk, metaStore);
    }

    /** Headless open by metastore alone (no ManagedLedger) -- for off-broker builds. */
    public static StreamLakeCatalog open(BookKeeper bk, StreamLakeMetaStore metaStore) {
        StreamLakeCatalog cat = new StreamLakeCatalog(bk, metaStore);
        try {
            cat.loadAndRotate();
        } catch (Exception e) {
            // A load failure when a catalog ledger was previously persisted is almost always transient
            // (e.g. a bookie operation timeout during recovery). Falling back to an EMPTY writable catalog
            // would silently make every query return 0 rows / 0 candidate ledgers -- and a later rotate
            // could overwrite the durable pointer -- so fail loudly and let the topic-load retry once the
            // bookie is healthy. Only start fresh when there is genuinely no prior catalog (a new topic).
            boolean hadPriorCatalog;
            try {
                hadPriorCatalog = metaStore.read().catalogLedgerId != null;
            } catch (Exception ignore) {
                hadPriorCatalog = false;
            }
            if (hadPriorCatalog) {
                throw new IllegalStateException("StreamLake catalog load failed for " + metaStore.name()
                        + " but a catalog ledger exists; refusing to serve an empty catalog (retryable)", e);
            }
            log.warn("StreamLake catalog starting fresh (no prior catalog) for {}: {}",
                    metaStore.name(), e.toString());
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

    /**
     * Mark a data ledger SEGMENTED and record where its segment (and page-index range) live, so a
     * query loads exactly that segment on demand without replaying the whole segment chain.
     */
    public synchronized void markSegmented(long dataLedgerId, long segLedgerId, long segStart, long segEnd,
            long piLedgerId, long piStart, long piEnd) {
        LedgerInfo cur = infos.get(dataLedgerId);
        if (cur == null) {
            cur = new LedgerInfo(dataLedgerId, System.currentTimeMillis(), Long.MAX_VALUE, Long.MIN_VALUE,
                    0, State.SEGMENTED, segLedgerId, segStart, segEnd, piLedgerId, piStart, piEnd);
        } else {
            cur = cur.segmented(segLedgerId, segStart, segEnd, piLedgerId, piStart, piEnd);
        }
        infos.put(dataLedgerId, cur);
        appendDurably(cur);
    }

    public LedgerInfo get(long dataLedgerId) {
        return infos.get(dataLedgerId);
    }

    /** Visits a candidate data ledger id; may throw so pruning work can run inline per ledger. */
    public interface LedgerVisitor {
        void visit(long dataLedgerId) throws Exception;
    }

    /**
     * Stream data ledgers whose [minEventTime, maxEventTime] intersects [fromMs, toMs] (date pruning) to
     * {@code visitor} without materializing the candidate list -- so a scan holds no per-query buffer
     * that scales with the number of matching ledgers (the resident catalog is iterated in place).
     */
    public void forEachCandidateLedger(long fromMs, long toMs, LedgerVisitor visitor) throws Exception {
        for (LedgerInfo i : infos.values()) {
            if (i.maxEventTime >= fromMs && i.minEventTime <= toMs) {
                visitor.visit(i.dataLedgerId);
            }
        }
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
                try {
                    long lac = old.getLastAddConfirmed();
                    // Bounded batches so a large catalog ledger cannot flood the bookie on load (same
                    // "too many read requests" flow-control that stalls a big page-index replay).
                    for (long start = 0; start <= lac; start += LOAD_READ_BATCH) {
                        long end = Math.min(start + LOAD_READ_BATCH - 1, lac);
                        Enumeration<LedgerEntry> en = old.readEntries(start, end);
                        while (en.hasMoreElements()) {
                            mergeEntry(en.nextElement().getEntry());
                        }
                    }
                } finally {
                    old.close();
                }
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
                log.warn("StreamLake catalog append failed for {}: {}", metaStore.name(), ex.toString());
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
                .putLong(i.maxEventTime).putLong(i.rowCount).put((byte) i.state.ordinal())
                .putLong(i.segmentLedgerId).putLong(i.segmentStartEntry).putLong(i.segmentEndEntry)
                .putLong(i.pageIndexLedgerId).putLong(i.pageIndexStartEntry).putLong(i.pageIndexEndEntry);
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
        long segLedgerId = bb.getLong();
        long segStart = bb.getLong();
        long segEnd = bb.getLong();
        long piLedgerId = bb.getLong();
        long piStart = bb.getLong();
        long piEnd = bb.getLong();
        return new LedgerInfo(id, createTs, minEt, maxEt, rows, state,
                segLedgerId, segStart, segEnd, piLedgerId, piStart, piEnd);
    }
}
