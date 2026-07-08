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
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.client.streaminglake.StreamLakeBatchStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The segment tier for a StreamLake topic (redesign): the coarse, in-memory read-side layer above the
 * per-page footers. A <b>segment</b> summarizes a contiguous run of a data ledger's entries with
 * merged per-column stats ({@link StreamLakeBatchStats}); a scan checks segments first to skip whole
 * entry ranges, then consults the per-page {@link StreamLakePageIndex} only within surviving segments.
 *
 * <p>Backed by a chain of BookKeeper ledgers whose ids live in the {@code /streamlake} node via
 * {@link StreamLakeMetaStore#setSegmentLedgerIds}. Entry format:
 * {@code 'S' | dataLedgerId(8) | startEntry(8) | endEntry(8) | statsLen(4) | stats}. Unlike the page
 * index, segments are kept in memory (they are small and are the hot pruning layer). Replayed on
 * {@link #open}; a fenced/closed head rolls to a fresh ledger.
 */
public class StreamLakeSegmentStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeSegmentStore.class);
    private static final byte[] PASSWORD = "streamlake-seg".getBytes();
    private static final byte ENTRY_SEGMENT = (byte) 'S';
    private static final int HEADER = 1 + 8 + 8 + 8 + 4;
    private static final long DEFAULT_MAX_HEAD_BYTES = 4L * 1024 * 1024;

    /** One segment: a contiguous [startEntry, endEntry] run of a data ledger and its merged stats. */
    public static final class Segment {
        public final long dataLedgerId;
        public final long startEntry;
        public final long endEntry;
        public final StreamLakeBatchStats stats;

        public Segment(long dataLedgerId, long startEntry, long endEntry, StreamLakeBatchStats stats) {
            this.dataLedgerId = dataLedgerId;
            this.startEntry = startEntry;
            this.endEntry = endEntry;
            this.stats = stats;
        }
    }

    private final BookKeeper bk;
    private final ManagedLedger ml;
    private final StreamLakeMetaStore metaStore;
    private final long maxHeadBytes;

    private final Map<Long, List<Segment>> byLedger = new HashMap<>();
    private final List<Long> chain = new ArrayList<>();
    private LedgerHandle head;
    private long headBytes;

    private StreamLakeSegmentStore(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                   long maxHeadBytes) {
        this.bk = bk;
        this.ml = ml;
        this.metaStore = metaStore;
        this.maxHeadBytes = maxHeadBytes > 0 ? maxHeadBytes : DEFAULT_MAX_HEAD_BYTES;
    }

    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore) {
        return open(bk, ml, metaStore, DEFAULT_MAX_HEAD_BYTES);
    }

    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                              long maxHeadBytes) {
        StreamLakeSegmentStore store = new StreamLakeSegmentStore(bk, ml, metaStore, maxHeadBytes);
        try {
            store.chain.addAll(metaStore.read().segmentLedgerIds);
            store.replay();
        } catch (Exception e) {
            log.warn("StreamLake segment store falling back to empty for {}: {}", ml.getName(), e.toString());
        }
        return store;
    }

    /** Durably append a segment and publish it in memory. */
    public synchronized void appendSegment(Segment segment) throws Exception {
        byte[] entry = encode(segment);
        ensureHeadFor(entry.length);
        addToHead(entry);
        headBytes += entry.length;
        byLedger.computeIfAbsent(segment.dataLedgerId, k -> new ArrayList<>()).add(segment);
    }

    /** The segments covering a data ledger (empty when it is not yet segmented). */
    public synchronized List<Segment> segmentsFor(long dataLedgerId) {
        List<Segment> segs = byLedger.get(dataLedgerId);
        return segs == null ? Collections.emptyList() : new ArrayList<>(segs);
    }

    public synchronized boolean covers(long dataLedgerId) {
        List<Segment> segs = byLedger.get(dataLedgerId);
        return segs != null && !segs.isEmpty();
    }

    @Override
    public synchronized void close() {
        if (head != null) {
            try {
                head.close();
            } catch (Exception ignore) {
                // recovery handles an unclosed head on the next load
            }
            head = null;
        }
    }

    // ---------------------------------------------------------------- replay

    private void replay() {
        for (long ledgerId : chain) {
            try {
                LedgerHandle lh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32, PASSWORD);
                long lac = lh.getLastAddConfirmed();
                if (lac >= 0) {
                    Enumeration<LedgerEntry> en = lh.readEntries(0, lac);
                    while (en.hasMoreElements()) {
                        parse(en.nextElement().getEntry());
                    }
                }
                lh.close();
            } catch (BKException.BKNoSuchLedgerExistsException
                    | BKException.BKNoSuchLedgerExistsOnMetadataServerException e) {
                // a chain entry was already GC'd; skip it
            } catch (Exception e) {
                log.warn("StreamLake segment store replay skipped ledger {} for {}: {}",
                        ledgerId, ml.getName(), e.toString());
            }
        }
    }

    private void parse(byte[] data) {
        if (data.length < HEADER || data[0] != ENTRY_SEGMENT) {
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.get(); // type
        long dataLedgerId = bb.getLong();
        long startEntry = bb.getLong();
        long endEntry = bb.getLong();
        int statsLen = bb.getInt();
        byte[] statsBytes = new byte[statsLen];
        bb.get(statsBytes);
        Segment seg = new Segment(dataLedgerId, startEntry, endEntry, StreamLakeBatchStats.decode(statsBytes));
        byLedger.computeIfAbsent(dataLedgerId, k -> new ArrayList<>()).add(seg);
    }

    // ---------------------------------------------------------------- write ledger chain

    private void ensureHeadFor(int entryLen) throws Exception {
        if (head == null || (headBytes > 0 && headBytes + entryLen > maxHeadBytes)) {
            rotateHead();
        }
    }

    private void rotateHead() throws Exception {
        if (head != null) {
            try {
                head.close();
            } catch (Exception ignore) {
                // best-effort
            }
        }
        LedgerHandle fresh = bk.createLedger(1, 1, BookKeeper.DigestType.CRC32, PASSWORD);
        chain.add(fresh.getId());
        metaStore.setSegmentLedgerIds(chain);
        head = fresh;
        headBytes = 0;
    }

    private void addToHead(byte[] entry) throws Exception {
        try {
            head.addEntry(entry);
        } catch (Exception e) {
            head = null;
            rotateHead();
            head.addEntry(entry);
        }
    }

    private static byte[] encode(Segment seg) {
        byte[] stats = seg.stats.encode();
        ByteBuffer bb = ByteBuffer.allocate(HEADER + stats.length);
        bb.put(ENTRY_SEGMENT);
        bb.putLong(seg.dataLedgerId);
        bb.putLong(seg.startEntry);
        bb.putLong(seg.endEntry);
        bb.putInt(stats.length);
        bb.put(stats);
        return bb.array();
    }
}
