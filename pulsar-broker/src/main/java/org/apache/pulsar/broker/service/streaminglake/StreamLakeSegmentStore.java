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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * A segment is a contiguous entry range in one ledger; {@link #appendLedgerSegment} returns that range
 * as {@code [segmentLedgerId, startEntry, endEntry]} for the catalog to point at. Segments are
 * <b>not</b> replayed en masse on open -- they are {@link #load loaded on demand} via their catalog
 * offset into a bounded LRU, so resident memory is O(cache) rather than O(all data ledgers). A
 * fenced/closed write head rolls to a fresh ledger.
 */
public class StreamLakeSegmentStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeSegmentStore.class);
    private static final byte[] PASSWORD = "streamlake-seg".getBytes();
    private static final byte ENTRY_DIRECTORY = (byte) 'D';
    private static final byte ENTRY_COLUMN = (byte) 'C';
    private static final long DEFAULT_MAX_HEAD_BYTES = 4L * 1024 * 1024;
    private static final int DEFAULT_CACHE_MAX_ENTRIES = 512;

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

    // Bounded LRU of loaded segments (keyed by data ledger id): segments are read on demand via their
    // catalog offset, not replayed en masse, so resident memory is O(cache) not O(all data ledgers).
    private final Map<Long, LedgerSegment> cache;
    private final List<Long> chain = new ArrayList<>();
    private LedgerHandle head;
    private long headBytes;

    private StreamLakeSegmentStore(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                   long maxHeadBytes, int ensembleSize, int writeQuorum, int ackQuorum,
                                   int cacheMaxEntries) {
        this.bk = bk;
        this.ml = ml;
        this.metaStore = metaStore;
        this.maxHeadBytes = maxHeadBytes > 0 ? maxHeadBytes : DEFAULT_MAX_HEAD_BYTES;
        this.ensembleSize = ensembleSize;
        this.writeQuorum = writeQuorum;
        this.ackQuorum = ackQuorum;
        int cap = cacheMaxEntries > 0 ? cacheMaxEntries : DEFAULT_CACHE_MAX_ENTRIES;
        this.cache = new LinkedHashMap<Long, LedgerSegment>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, LedgerSegment> eldest) {
                return size() > cap;
            }
        };
    }

    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore) {
        return open(bk, ml, metaStore, DEFAULT_MAX_HEAD_BYTES);
    }

    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                              long maxHeadBytes) {
        return open(bk, ml, metaStore, maxHeadBytes, 1, 1, 1);
    }

    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
            long maxHeadBytes, int ensembleSize, int writeQuorum, int ackQuorum) {
        return open(bk, ml, metaStore, maxHeadBytes, ensembleSize, writeQuorum, ackQuorum,
                DEFAULT_CACHE_MAX_ENTRIES);
    }

    /**
     * Open with an explicit replication and segment cache size. Segments are <b>not</b> replayed into
     * memory on open -- they are loaded on demand via their catalog offset ({@link #load}) and held in
     * a bounded LRU, so resident memory does not grow with the number of data ledgers. The chain is
     * loaded only for write-head/GC management.
     */
    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
            long maxHeadBytes, int ensembleSize, int writeQuorum, int ackQuorum, int cacheMaxEntries) {
        StreamLakeSegmentStore store = new StreamLakeSegmentStore(bk, ml, metaStore, maxHeadBytes,
                ensembleSize, writeQuorum, ackQuorum, cacheMaxEntries);
        try {
            store.chain.addAll(metaStore.read().segmentLedgerIds);
        } catch (Exception e) {
            log.warn("StreamLake segment store falling back to empty for {}: {}", ml.getName(), e.toString());
        }
        return store;
    }

    /**
     * Durably append a data ledger's segment (page directory + per-column segments) as a contiguous run
     * of entries in one segment ledger, and return its offset {@code [segmentLedgerId, startEntry,
     * endEntry]} so the catalog can point a query straight at it.
     */
    public synchronized long[] appendLedgerSegment(long dataLedgerId, long[] pageEntryIds,
            List<StreamLakeColumnSegment> columns) throws Exception {
        byte[] dir = encodeDirectory(dataLedgerId, pageEntryIds);
        byte[][] colEntries = new byte[columns.size()][];
        long total = dir.length;
        for (int c = 0; c < columns.size(); c++) {
            colEntries[c] = encodeColumn(dataLedgerId, columns.get(c));
            total += colEntries[c].length;
        }
        // Keep the whole segment in one ledger so its offset is a single contiguous range.
        ensureHeadFor((int) Math.min(Integer.MAX_VALUE, total));
        long segmentLedgerId = head.getId();
        long startEntry = addToHead(dir);
        headBytes += dir.length;
        Map<Integer, StreamLakeColumnSegment> cols = new HashMap<>();
        long endEntry = startEntry;
        for (int c = 0; c < columns.size(); c++) {
            endEntry = addToHead(colEntries[c]);
            headBytes += colEntries[c].length;
            cols.put(columns.get(c).columnIndex(), columns.get(c));
        }
        cache.put(dataLedgerId, new LedgerSegment(dataLedgerId, pageEntryIds, cols));
        return new long[]{segmentLedgerId, startEntry, endEntry};
    }

    /**
     * Load a data ledger's segment on demand from its catalog offset (a contiguous entry range in a
     * segment ledger), caching it in the bounded LRU. Returns {@code null} if the range can't be read.
     */
    public synchronized LedgerSegment load(long dataLedgerId, long segmentLedgerId, long startEntry,
            long endEntry) throws Exception {
        LedgerSegment cached = cache.get(dataLedgerId);
        if (cached != null) {
            return cached;
        }
        if (segmentLedgerId < 0 || startEntry < 0 || endEntry < startEntry) {
            return null;
        }
        LedgerHandle lh = bk.openLedger(segmentLedgerId, BookKeeper.DigestType.CRC32, PASSWORD);
        try {
            long[] pageEntryIds = null;
            Map<Integer, StreamLakeColumnSegment> cols = new HashMap<>();
            java.util.Enumeration<LedgerEntry> en = lh.readEntries(startEntry, endEntry);
            while (en.hasMoreElements()) {
                byte[] data = en.nextElement().getEntry();
                ByteBuffer bb = ByteBuffer.wrap(data);
                byte type = bb.get();
                bb.getLong(); // dataLedgerId (already known)
                if (type == ENTRY_DIRECTORY) {
                    int numPages = bb.getInt();
                    pageEntryIds = new long[numPages];
                    for (int i = 0; i < numPages; i++) {
                        pageEntryIds[i] = bb.getLong();
                    }
                } else if (type == ENTRY_COLUMN) {
                    int blobLen = bb.getInt();
                    byte[] blob = new byte[blobLen];
                    bb.get(blob);
                    StreamLakeColumnSegment cseg = StreamLakeColumnSegment.decode(blob);
                    cols.put(cseg.columnIndex(), cseg);
                }
            }
            if (pageEntryIds == null) {
                return null;
            }
            LedgerSegment seg = new LedgerSegment(dataLedgerId, pageEntryIds, cols);
            cache.put(dataLedgerId, seg);
            return seg;
        } finally {
            try {
                lh.close();
            } catch (Exception ignore) {
                // best-effort
            }
        }
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

    /** Number of segments currently resident in the bounded LRU cache (observability/tests). */
    public synchronized int cachedSegmentCount() {
        return cache.size();
    }

    // ---------------------------------------------------------------- write ledger chain

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

    private long addToHead(byte[] entry) throws Exception {
        try {
            return head.addEntry(entry);
        } catch (Exception e) {
            head = null;
            rotateHead();
            return head.addEntry(entry);
        }
    }
}
