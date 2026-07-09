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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.client.streaminglake.StreamLakeColumnSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The segment tier for a StreamLake topic: the in-memory, column-oriented read-side index above the
 * per-page footers. A data ledger's segment is a <b>page directory</b> (position -&gt; data entryId)
 * plus one {@link StreamLakeColumnSegment} per indexed column, each holding that column's per-page
 * stats. A scan prunes to the exact candidate pages from the column segments alone -- once a ledger is
 * segmented, pruning no longer reads the per-page {@link StreamLakePageIndex} at all.
 *
 * <p>Backed by a chain of BookKeeper ledgers whose ids live in the {@code /streamlake} node via
 * {@link StreamLakeMetaStore#setSegmentLedgerIds}. A data ledger's segment is written as one directory
 * entry followed by one entry per column:
 * <pre>
 *   'D' | dataLedgerId(8) | numPages(4) | pageEntryId(8) x numPages
 *   'C' | dataLedgerId(8) | blobLen(4)  | StreamLakeColumnSegment.encode()
 * </pre>
 * Segments are kept in memory (small, hot pruning layer); replayed on {@link #open}. A fenced/closed
 * head rolls to a fresh ledger.
 */
public class StreamLakeSegmentStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeSegmentStore.class);
    private static final byte[] PASSWORD = "streamlake-seg".getBytes();
    private static final byte ENTRY_DIRECTORY = (byte) 'D';
    private static final byte ENTRY_COLUMN = (byte) 'C';
    private static final long DEFAULT_MAX_HEAD_BYTES = 4L * 1024 * 1024;

    /** A data ledger's segment: the page directory (position -&gt; entryId) + per-column segments. */
    public static final class LedgerSegment {
        public final long dataLedgerId;
        public final long[] pageEntryIds;
        public final Map<Integer, StreamLakeColumnSegment> columns;

        public LedgerSegment(long dataLedgerId, long[] pageEntryIds,
                Map<Integer, StreamLakeColumnSegment> columns) {
            this.dataLedgerId = dataLedgerId;
            this.pageEntryIds = pageEntryIds;
            this.columns = columns;
        }

        public int numPages() {
            return pageEntryIds.length;
        }
    }

    private final BookKeeper bk;
    private final ManagedLedger ml;
    private final StreamLakeMetaStore metaStore;
    private final long maxHeadBytes;
    private final int ensembleSize;
    private final int writeQuorum;
    private final int ackQuorum;

    private final Map<Long, LedgerSegment> byLedger = new HashMap<>();
    private final List<Long> chain = new ArrayList<>();
    private LedgerHandle head;
    private long headBytes;

    private StreamLakeSegmentStore(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                   long maxHeadBytes, int ensembleSize, int writeQuorum, int ackQuorum) {
        this.bk = bk;
        this.ml = ml;
        this.metaStore = metaStore;
        this.maxHeadBytes = maxHeadBytes > 0 ? maxHeadBytes : DEFAULT_MAX_HEAD_BYTES;
        this.ensembleSize = ensembleSize;
        this.writeQuorum = writeQuorum;
        this.ackQuorum = ackQuorum;
    }

    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore) {
        return open(bk, ml, metaStore, DEFAULT_MAX_HEAD_BYTES);
    }

    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                              long maxHeadBytes) {
        return open(bk, ml, metaStore, maxHeadBytes, 1, 1, 1);
    }

    /** Open with an explicit replication (production: higher RF for read-scalable pruning). */
    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
            long maxHeadBytes, int ensembleSize, int writeQuorum, int ackQuorum) {
        StreamLakeSegmentStore store = new StreamLakeSegmentStore(bk, ml, metaStore, maxHeadBytes,
                ensembleSize, writeQuorum, ackQuorum);
        try {
            store.chain.addAll(metaStore.read().segmentLedgerIds);
            store.replay();
        } catch (Exception e) {
            log.warn("StreamLake segment store falling back to empty for {}: {}", ml.getName(), e.toString());
        }
        return store;
    }

    /** Durably append a data ledger's segment (page directory + per-column segments) and publish it. */
    public synchronized void appendLedgerSegment(long dataLedgerId, long[] pageEntryIds,
            List<StreamLakeColumnSegment> columns) throws Exception {
        byte[] dir = encodeDirectory(dataLedgerId, pageEntryIds);
        ensureHeadFor(dir.length);
        addToHead(dir);
        headBytes += dir.length;
        Map<Integer, StreamLakeColumnSegment> cols = new HashMap<>();
        for (StreamLakeColumnSegment cseg : columns) {
            byte[] entry = encodeColumn(dataLedgerId, cseg);
            ensureHeadFor(entry.length);
            addToHead(entry);
            headBytes += entry.length;
            cols.put(cseg.columnIndex(), cseg);
        }
        byLedger.put(dataLedgerId, new LedgerSegment(dataLedgerId, pageEntryIds, cols));
    }

    /** The segment for a data ledger, or {@code null} when it is not yet segmented. */
    public synchronized LedgerSegment segmentFor(long dataLedgerId) {
        return byLedger.get(dataLedgerId);
    }

    public synchronized boolean covers(long dataLedgerId) {
        return byLedger.containsKey(dataLedgerId);
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
        if (data.length < 1 + 8) {
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(data);
        byte type = bb.get();
        long dataLedgerId = bb.getLong();
        if (type == ENTRY_DIRECTORY) {
            int numPages = bb.getInt();
            long[] pageEntryIds = new long[numPages];
            for (int i = 0; i < numPages; i++) {
                pageEntryIds[i] = bb.getLong();
            }
            // A directory opens (or replaces, on a re-segment) a data ledger's segment.
            byLedger.put(dataLedgerId, new LedgerSegment(dataLedgerId, pageEntryIds, new HashMap<>()));
        } else if (type == ENTRY_COLUMN) {
            int blobLen = bb.getInt();
            byte[] blob = new byte[blobLen];
            bb.get(blob);
            LedgerSegment seg = byLedger.get(dataLedgerId);
            if (seg != null) { // directory precedes its columns
                StreamLakeColumnSegment cseg = StreamLakeColumnSegment.decode(blob);
                seg.columns.put(cseg.columnIndex(), cseg);
            }
        }
    }

    private static byte[] encodeDirectory(long dataLedgerId, long[] pageEntryIds) {
        ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 4 + pageEntryIds.length * 8);
        bb.put(ENTRY_DIRECTORY);
        bb.putLong(dataLedgerId);
        bb.putInt(pageEntryIds.length);
        for (long e : pageEntryIds) {
            bb.putLong(e);
        }
        return bb.array();
    }

    private static byte[] encodeColumn(long dataLedgerId, StreamLakeColumnSegment cseg) {
        byte[] blob = cseg.encode();
        ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 4 + blob.length);
        bb.put(ENTRY_COLUMN);
        bb.putLong(dataLedgerId);
        bb.putInt(blob.length);
        bb.put(blob);
        return bb.array();
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
        LedgerHandle fresh = bk.createLedger(ensembleSize, writeQuorum, ackQuorum,
                BookKeeper.DigestType.CRC32, PASSWORD);
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
}
