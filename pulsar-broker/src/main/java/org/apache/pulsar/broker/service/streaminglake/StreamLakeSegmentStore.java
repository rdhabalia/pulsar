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
import org.apache.pulsar.client.streaminglake.StreamLakeBatchStats;
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
    // A single coarse whole-ledger min/max-per-column stats entry, written first in each segment so a
    // query can reject a data ledger by reading just this one small entry (no full segment load).
    private static final byte ENTRY_SUMMARY = (byte) 'S';
    private static final int DEFAULT_SUMMARY_CACHE_MAX = 65_536;
    private static final long DEFAULT_MAX_HEAD_BYTES = 4L * 1024 * 1024;
    private static final int DEFAULT_CACHE_MAX_ENTRIES = 512;
    private static final int DEFAULT_MAX_ENTRIES_PER_LEDGER = 200_000;

    /** A data ledger's segment: the page directory (position -&gt; entryId) + per-column segments. */
    public static final class LedgerSegment {
        public final long dataLedgerId;
        public final long[] pageEntryIds;
        public final Map<Integer, StreamLakeColumnSegment> columns;
        /** Total segment-ledger bytes read to load this segment (directory + column blobs). */
        public final long sizeBytes;

        public LedgerSegment(long dataLedgerId, long[] pageEntryIds,
                Map<Integer, StreamLakeColumnSegment> columns) {
            this(dataLedgerId, pageEntryIds, columns, 0);
        }

        public LedgerSegment(long dataLedgerId, long[] pageEntryIds,
                Map<Integer, StreamLakeColumnSegment> columns, long sizeBytes) {
            this.dataLedgerId = dataLedgerId;
            this.pageEntryIds = pageEntryIds;
            this.columns = columns;
            this.sizeBytes = sizeBytes;
        }

        public int numPages() {
            return pageEntryIds.length;
        }
    }

    private final BookKeeper bk;
    private final StreamLakeMetaStore metaStore;
    private final long maxHeadBytes;
    private final int maxEntriesPerLedger;
    private final int ensembleSize;
    private final int writeQuorum;
    private final int ackQuorum;

    // Bounded LRU of loaded segments (keyed by data ledger id): segments are read on demand via their
    // catalog offset, not replayed en masse, so resident memory is O(cache) not O(all data ledgers).
    private final Map<Long, LedgerSegment> cache;
    // Bounded LRU of coarse per-ledger summaries (keyed by data ledger id): tiny (one min/max per column),
    // so a much larger cap is affordable -- lets repeat/selective queries reject ledgers with zero reads.
    private final Map<Long, StreamLakeBatchStats> summaryCache;
    // Cached read handles keyed by segment-ledger id: many data ledgers' segments/summaries live in one
    // segment ledger, so opening it once and reusing the handle across candidates avoids an openLedger
    // round-trip per candidate (the cost that otherwise dwarfs the tiny summary read).
    private final Map<Long, LedgerHandle> readHandles = new HashMap<>();
    private final List<Long> chain = new ArrayList<>();
    private LedgerHandle head;
    private long headBytes;
    private int headEntryCount;

    private StreamLakeSegmentStore(BookKeeper bk, StreamLakeMetaStore metaStore,
                                   long maxHeadBytes, int maxEntriesPerLedger, int ensembleSize,
                                   int writeQuorum, int ackQuorum, int cacheMaxEntries) {
        this.bk = bk;
        this.metaStore = metaStore;
        this.maxHeadBytes = maxHeadBytes > 0 ? maxHeadBytes : DEFAULT_MAX_HEAD_BYTES;
        this.maxEntriesPerLedger = maxEntriesPerLedger > 0 ? maxEntriesPerLedger
                : DEFAULT_MAX_ENTRIES_PER_LEDGER;
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
        this.summaryCache = new LinkedHashMap<Long, StreamLakeBatchStats>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, StreamLakeBatchStats> eldest) {
                return size() > DEFAULT_SUMMARY_CACHE_MAX;
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

    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
            long maxHeadBytes, int ensembleSize, int writeQuorum, int ackQuorum, int cacheMaxEntries) {
        return open(bk, ml, metaStore, maxHeadBytes, DEFAULT_MAX_ENTRIES_PER_LEDGER, ensembleSize,
                writeQuorum, ackQuorum, cacheMaxEntries);
    }

    /**
     * Open with an explicit replication, entry-based roll threshold and segment cache size. One segment
     * ledger holds many data ledgers' segments (rolls at {@code maxEntriesPerLedger}); segments are
     * <b>not</b> replayed into memory on open -- they are loaded on demand via their catalog offset
     * ({@link #load}) into a bounded LRU, so resident memory does not grow with the number of data
     * ledgers. The chain is loaded only for write-head/GC management.
     */
    public static StreamLakeSegmentStore open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
            long maxHeadBytes, int maxEntriesPerLedger, int ensembleSize, int writeQuorum, int ackQuorum,
            int cacheMaxEntries) {
        return open(bk, metaStore, maxHeadBytes, maxEntriesPerLedger, ensembleSize, writeQuorum, ackQuorum,
                cacheMaxEntries);
    }

    /** Headless open by metastore alone (no ManagedLedger) -- for off-broker builds. */
    public static StreamLakeSegmentStore open(BookKeeper bk, StreamLakeMetaStore metaStore,
            long maxHeadBytes, int maxEntriesPerLedger, int ensembleSize, int writeQuorum, int ackQuorum,
            int cacheMaxEntries) {
        StreamLakeSegmentStore store = new StreamLakeSegmentStore(bk, metaStore, maxHeadBytes,
                maxEntriesPerLedger, ensembleSize, writeQuorum, ackQuorum, cacheMaxEntries);
        try {
            store.chain.addAll(metaStore.read().segmentLedgerIds);
        } catch (Exception e) {
            log.warn("StreamLake segment store falling back to empty for {}: {}",
                    metaStore.name(), e.toString());
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
        // Coarse whole-ledger summary (one min/max per column), written FIRST so a query can reject this
        // data ledger by reading only this small entry instead of loading all the per-column entries.
        StreamLakeBatchStats summary = buildSummary(columns);
        byte[] summaryEntryBytes = encodeSummary(dataLedgerId, summary);
        byte[] dir = encodeDirectory(dataLedgerId, pageEntryIds);
        byte[][] colEntries = new byte[columns.size()][];
        long total = (long) summaryEntryBytes.length + dir.length;
        for (int c = 0; c < columns.size(); c++) {
            colEntries[c] = encodeColumn(dataLedgerId, columns.get(c));
            total += colEntries[c].length;
        }
        // Keep the whole segment (summary + directory + columns) in one ledger so its offset is a single
        // contiguous range [startEntry(=summary) .. endEntry].
        ensureHeadFor((int) Math.min(Integer.MAX_VALUE, total), 2 + columns.size());
        long segmentLedgerId = head.getId();
        long startEntry = addToHead(summaryEntryBytes);
        headBytes += summaryEntryBytes.length;
        headEntryCount++;
        long endEntry = addToHead(dir);
        headBytes += dir.length;
        headEntryCount++;
        Map<Integer, StreamLakeColumnSegment> cols = new HashMap<>();
        for (int c = 0; c < columns.size(); c++) {
            endEntry = addToHead(colEntries[c]);
            headBytes += colEntries[c].length;
            headEntryCount++;
            cols.put(columns.get(c).columnIndex(), columns.get(c));
        }
        cache.put(dataLedgerId, new LedgerSegment(dataLedgerId, pageEntryIds, cols));
        summaryCache.put(dataLedgerId, summary);
        return new long[]{segmentLedgerId, startEntry, endEntry};
    }

    /** Coarse whole-ledger stats (one min/max per column, no set/bloom) for the segment's summary entry. */
    private static StreamLakeBatchStats buildSummary(List<StreamLakeColumnSegment> columns) {
        List<StreamLakeBatchStats.ColumnStats> cs = new ArrayList<>(columns.size());
        for (StreamLakeColumnSegment c : columns) {
            cs.add(StreamLakeBatchStats.minMaxColumn(c.columnIndex(), c.type(), c.wholeMin(), c.wholeMax()));
        }
        return StreamLakeBatchStats.of(cs);
    }

    /** A cached read handle for a segment ledger. Reuses the write {@code head} for the active head so a
     * query never fences the ledger the builder is still appending to; other ledgers are opened once. */
    private LedgerHandle readHandle(long segmentLedgerId) throws Exception {
        if (head != null && head.getId() == segmentLedgerId) {
            return head;
        }
        LedgerHandle lh = readHandles.get(segmentLedgerId);
        if (lh == null) {
            lh = bk.openLedger(segmentLedgerId, BookKeeper.DigestType.CRC32, PASSWORD);
            readHandles.put(segmentLedgerId, lh);
        }
        return lh;
    }

    /**
     * Load a data ledger's coarse summary (whole-ledger min/max per column) from the FIRST entry of its
     * segment range -- a single small read used to reject a ledger without loading the full segment.
     * Returns {@code null} if there is no summary (then the caller falls back to the full segment).
     */
    public synchronized StreamLakeBatchStats loadSummary(long dataLedgerId, long segmentLedgerId,
            long summaryEntry) throws Exception {
        StreamLakeBatchStats cached = summaryCache.get(dataLedgerId);
        if (cached != null) {
            return cached;
        }
        if (segmentLedgerId < 0 || summaryEntry < 0) {
            return null;
        }
        LedgerHandle lh = readHandle(segmentLedgerId);
        java.util.Enumeration<LedgerEntry> en = lh.readEntries(summaryEntry, summaryEntry);
        if (!en.hasMoreElements()) {
            return null;
        }
        byte[] data = en.nextElement().getEntry();
        if (data.length < 1 + 8 + 4 || data[0] != ENTRY_SUMMARY) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.get();           // type
        bb.getLong();       // dataLedgerId (already known)
        int blobLen = bb.getInt();
        byte[] blob = new byte[blobLen];
        bb.get(blob);
        StreamLakeBatchStats summary = StreamLakeBatchStats.decode(blob);
        summaryCache.put(dataLedgerId, summary);
        return summary;
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
        LedgerHandle lh = readHandle(segmentLedgerId);
        long[] pageEntryIds = null;
        Map<Integer, StreamLakeColumnSegment> cols = new HashMap<>();
        long bytesRead = 0;
        java.util.Enumeration<LedgerEntry> en = lh.readEntries(startEntry, endEntry);
        while (en.hasMoreElements()) {
            byte[] data = en.nextElement().getEntry();
            bytesRead += data.length;
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
        LedgerSegment seg = new LedgerSegment(dataLedgerId, pageEntryIds, cols, bytesRead);
        cache.put(dataLedgerId, seg);
        return seg;
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
        for (LedgerHandle lh : readHandles.values()) {
            try {
                lh.close();
            } catch (Exception ignore) {
                // best-effort
            }
        }
        readHandles.clear();
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

    private static byte[] encodeSummary(long dataLedgerId, StreamLakeBatchStats summary) {
        byte[] blob = summary.encode();
        ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 4 + blob.length);
        bb.put(ENTRY_SUMMARY);
        bb.putLong(dataLedgerId);
        bb.putInt(blob.length);
        bb.put(blob);
        return bb.array();
    }

    private void ensureHeadFor(int entryLen, int entriesToAdd) throws Exception {
        if (head == null) {
            rotateHead();
            return;
        }
        boolean overBytes = headBytes > 0 && headBytes + entryLen > maxHeadBytes;
        boolean overEntries = headEntryCount > 0 && headEntryCount + entriesToAdd > maxEntriesPerLedger;
        if (overBytes || overEntries) {
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
        headEntryCount = 0;
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
