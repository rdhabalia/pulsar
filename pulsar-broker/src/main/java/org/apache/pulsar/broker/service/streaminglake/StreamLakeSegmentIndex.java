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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.PageStatEntry;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segment-level metadata index for a StreamLake topic (design: the coarse layer between date-partition
 * pruning and the bookie {@code PAGE_PRUNE}). A <b>segment</b> summarizes {@code pagesPerSegment}
 * consecutive pages of one data ledger (merged min/max, and exact set for low-cardinality columns);
 * a scan can skip whole segments before touching the per-page index.
 *
 * <p>Build path (metadata only): {@link #buildForClosedLedger} windows the bookie {@code PAGE_STATS}
 * API over a closed ledger's page-index blobs (never the data), stream-merges them into segments,
 * and appends them to a durable index-ledger.
 *
 * <p>Storage: a <b>chain</b> of index-ledgers whose ids live in the {@code /streamlake} node (via
 * {@link StreamLakeMetaStore}), never in the managed-ledger znode. Entries are typed: a
 * <b>segment</b> entry ({@code 'S'}) or a per-data-ledger <b>commit marker</b> ({@code 'C'}) written
 * after all of that ledger's segments. On {@link #open} the chain is replayed and only ledgers whose
 * commit marker matches their segment count are exposed — a partially built (crashed) ledger is
 * ignored and re-compacted, so the scan never sees partial coverage.
 *
 * <p>Read path: {@link #segmentsFor} returns a data ledger's segments (empty when not yet covered,
 * so the scan falls back to a full page prune). Correctness never depends on the index being current.
 */
public class StreamLakeSegmentIndex {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeSegmentIndex.class);
    private static final byte[] PASSWORD = "streamlake-seg".getBytes();
    private static final byte ENTRY_SEGMENT = (byte) 'S';
    private static final byte ENTRY_COMMIT = (byte) 'C';
    private static final long OP_TIMEOUT_SEC = 30;

    private final BookKeeper bk;
    private final ManagedLedger ml;
    private final StreamLakeMetaStore metaStore;
    private final StreamingLakeConfig config;

    private final Map<Long, List<SegmentSummary>> byLedger = new HashMap<>();
    private final Set<Long> committed = new HashSet<>();
    private final List<Long> segmentLedgerIds = new ArrayList<>();
    private LedgerHandle head; // lazily created write ledger (chain tail)

    private StreamLakeSegmentIndex(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                   StreamingLakeConfig config) {
        this.bk = bk;
        this.ml = ml;
        this.metaStore = metaStore;
        this.config = config;
    }

    /** Load and replay the segment-ledger chain; never throws (falls back to an empty index). */
    public static StreamLakeSegmentIndex open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                              StreamingLakeConfig config) {
        StreamLakeSegmentIndex idx = new StreamLakeSegmentIndex(bk, ml, metaStore, config);
        try {
            List<Long> chain = metaStore.read().segmentLedgerIds;
            idx.segmentLedgerIds.addAll(chain);
            idx.replay(chain);
        } catch (Exception e) {
            log.warn("StreamLake segment index falling back to empty for {}: {}", ml.getName(), e.toString());
        }
        return idx;
    }

    /** The committed segments for a data ledger (empty when the ledger is not yet indexed). */
    public synchronized List<SegmentSummary> segmentsFor(long dataLedgerId) {
        return byLedger.getOrDefault(dataLedgerId, Collections.emptyList());
    }

    /** Whether a data ledger has a complete, committed segment set. */
    public synchronized boolean isCovered(long dataLedgerId) {
        return committed.contains(dataLedgerId);
    }

    /**
     * Compact a closed data ledger into segments: window {@code PAGE_STATS} over [0, lastEntry],
     * merge {@code pagesPerSegment} pages per segment, append the segments and a commit marker to
     * the index-ledger chain, and publish them in memory. Idempotent — a ledger already committed is
     * skipped. Reads only the page index, never the data.
     */
    public synchronized void buildForClosedLedger(long dataLedgerId, long lastEntry) throws Exception {
        if (committed.contains(dataLedgerId) || lastEntry < 0) {
            return;
        }
        int pagesPerSegment = Math.max(1, config.getPagesPerSegment());
        int setCap = config.getSetMaxCardinality();
        BookieClient bookieClient = bk.getClientCtx().getBookieClient();
        BookieId bookie = resolveBookie(dataLedgerId);

        List<SegmentSummary> segments = new ArrayList<>();
        for (long start = 0; start <= lastEntry; start += pagesPerSegment) {
            long end = Math.min(start + pagesPerSegment - 1, lastEntry);
            List<PageStatEntry> stats = bookieClient.pageStats(bookie, dataLedgerId, start, end)
                    .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            if (stats.isEmpty()) {
                continue;
            }
            SegmentSummary.Builder b = new SegmentSummary.Builder(dataLedgerId, setCap);
            for (PageStatEntry stat : stats) {
                b.addPage(stat.getEntryId(), 0, stat.getBlob());
            }
            segments.add(b.build());
        }

        // durably append all segments, then the commit marker (only a matching marker makes them live)
        for (SegmentSummary seg : segments) {
            appendEntry(segmentEntry(seg));
        }
        appendEntry(commitEntry(dataLedgerId, segments.size()));

        byLedger.put(dataLedgerId, segments);
        committed.add(dataLedgerId);
    }

    public synchronized void close() {
        if (head != null) {
            try {
                head.close();
            } catch (Exception ignore) {
                // openLedger recovery handles an unclosed ledger on the next load
            }
            head = null;
        }
    }

    // ---------------------------------------------------------------- replay

    private void replay(List<Long> chain) {
        Map<Long, List<SegmentSummary>> pending = new HashMap<>();
        for (long ledgerId : chain) {
            try {
                LedgerHandle lh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32, PASSWORD);
                long lac = lh.getLastAddConfirmed();
                if (lac >= 0) {
                    Enumeration<LedgerEntry> en = lh.readEntries(0, lac);
                    while (en.hasMoreElements()) {
                        parseEntry(en.nextElement().getEntry(), pending);
                    }
                }
                lh.close();
            } catch (BKException.BKNoSuchLedgerExistsException
                    | BKException.BKNoSuchLedgerExistsOnMetadataServerException e) {
                // a chain entry was already GC'd; skip it
            } catch (Exception e) {
                log.warn("StreamLake segment index replay skipped ledger {} for {}: {}",
                        ledgerId, ml.getName(), e.toString());
            }
        }
    }

    private void parseEntry(byte[] data, Map<Long, List<SegmentSummary>> pending) {
        ByteBuffer bb = ByteBuffer.wrap(data);
        byte type = bb.get();
        if (type == ENTRY_SEGMENT) {
            int partIndex = bb.getShort() & 0xFFFF;
            int partCount = bb.getShort() & 0xFFFF;
            if (partCount != 1) {
                // multi-part segments are not produced yet; ignore until the chunker lands
                log.warn("StreamLake segment index: skipping multi-part segment (part {}/{}) for {}",
                        partIndex, partCount, ml.getName());
                return;
            }
            byte[] blob = new byte[bb.remaining()];
            bb.get(blob);
            SegmentSummary seg = SegmentSummaryCodec.decode(blob);
            pending.computeIfAbsent(seg.coversLedgerId(), k -> new ArrayList<>()).add(seg);
        } else if (type == ENTRY_COMMIT) {
            long coversLedgerId = bb.getLong();
            int segmentCount = bb.getInt();
            List<SegmentSummary> segs = pending.getOrDefault(coversLedgerId, Collections.emptyList());
            if (segs.size() == segmentCount) {
                byLedger.put(coversLedgerId, new ArrayList<>(segs));
                committed.add(coversLedgerId);
            }
            pending.remove(coversLedgerId);
        }
    }

    // ---------------------------------------------------------------- write ledger chain

    private void appendEntry(byte[] data) throws Exception {
        ensureHead();
        try {
            head.addEntry(data);
        } catch (Exception e) {
            // head fenced/closed -> start a new head and retry once
            rotateHead();
            head.addEntry(data);
        }
    }

    private void ensureHead() throws Exception {
        if (head == null) {
            rotateHead();
        }
    }

    private void rotateHead() throws Exception {
        LedgerHandle fresh = bk.createLedger(1, 1, BookKeeper.DigestType.CRC32, PASSWORD);
        segmentLedgerIds.add(fresh.getId());
        metaStore.setSegmentLedgerIds(segmentLedgerIds);
        head = fresh;
    }

    private BookieId resolveBookie(long ledgerId) throws Exception {
        return bk.getLedgerManager().readLedgerMetadata(ledgerId).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS)
                .getValue().getAllEnsembles().firstEntry().getValue().get(0);
    }

    // ---------------------------------------------------------------- entry codec

    /** Segment entry: 'S' | partIndex(2) | partCount(2) | SegmentSummary blob. */
    private static byte[] segmentEntry(SegmentSummary seg) {
        byte[] blob = SegmentSummaryCodec.encode(seg);
        ByteBuffer bb = ByteBuffer.allocate(1 + 2 + 2 + blob.length);
        bb.put(ENTRY_SEGMENT);
        bb.putShort((short) 0); // partIndex  (single-part until the multi-part chunker lands)
        bb.putShort((short) 1); // partCount
        bb.put(blob);
        return bb.array();
    }

    /** Commit marker: 'C' | coversLedgerId(8) | segmentCount(4). */
    private static byte[] commitEntry(long coversLedgerId, int segmentCount) {
        ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 4);
        bb.put(ENTRY_COMMIT);
        bb.putLong(coversLedgerId);
        bb.putInt(segmentCount);
        return bb.array();
    }
}
