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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.BloomFilter;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * StreamLake predicate scan over a live batched topic (design points 10-13): push the column
 * predicate to each ledger's bookie via {@code PAGE_PRUNE}, then read only the surviving page
 * entries, decode them, and keep just the rows that match the exact predicate.
 *
 * <p>(Date-partition pruning -- point 9 -- would prepend a date-ledger filter; that index is not
 * yet maintained on the live topic, so this scans all of the topic's ledgers.)
 */
public final class StreamLakePageScan {

    private StreamLakePageScan() {
    }

    /**
     * An INT-column predicate: an exclusive lower ({@code gt}) and/or upper ({@code lt}) range bound,
     * or an exact equality ({@code eq}). Equality engages the per-granule exact set index, which
     * prunes a granule even when the value falls inside its min/max.
     */
    public static final class Bound {
        public final short columnId;
        public final String columnName;
        public final Integer gt;
        public final Integer lt;
        public final Integer eq;

        public Bound(short columnId, String columnName, Integer gt, Integer lt) {
            this(columnId, columnName, gt, lt, null);
        }

        public Bound(short columnId, String columnName, Integer gt, Integer lt, Integer eq) {
            this.columnId = columnId;
            this.columnName = columnName;
            this.gt = gt;
            this.lt = lt;
            this.eq = eq;
        }

        /** An equality predicate {@code column == value}. */
        public static Bound eq(short columnId, String columnName, int value) {
            return new Bound(columnId, columnName, null, null, value);
        }
    }

    /** Scan result: matching row payloads plus how many ledgers were scanned vs date-pruned. */
    public static final class ScanResult {
        public final List<byte[]> rows;
        public final int ledgersScanned;
        public final int ledgersPrunedByDate;

        ScanResult(List<byte[]> rows, int ledgersScanned, int ledgersPrunedByDate) {
            this.rows = rows;
            this.ledgersScanned = ledgersScanned;
            this.ledgersPrunedByDate = ledgersPrunedByDate;
        }
    }

    /** @return the value payloads of every row across the topic matching all bounds. */
    public static List<byte[]> scan(PersistentTopic topic, BookKeeper bk, List<Bound> bounds)
            throws Exception {
        return scan(topic, bk, bounds, Long.MIN_VALUE, Long.MAX_VALUE).rows;
    }

    /**
     * Three-level prune: (9) skip ledgers whose date range is outside [{@code fromDate},
     * {@code toDate}], (10) bookie PAGE_PRUNE on column ranges, (12) selective row decode.
     */
    public static ScanResult scan(PersistentTopic topic, BookKeeper bk, List<Bound> bounds,
                                  long fromDate, long toDate) throws Exception {
        return scan(topic, bk, bounds, java.util.Collections.emptyMap(), fromDate, toDate);
    }

    /**
     * As {@link #scan(PersistentTopic, BookKeeper, List, long, long)} but also pushes a per-column
     * key-set (semi-join probe keys) to the bookie, which tests them against each page's value bloom.
     */
    public static ScanResult scan(PersistentTopic topic, BookKeeper bk, List<Bound> bounds,
                                  Map<Short, List<byte[]>> keySets, long fromDate, long toDate)
            throws Exception {
        RowResult r = scanRows(topic, bk, bounds, keySets, fromDate, toDate);
        List<byte[]> values = new ArrayList<>(r.rows.size());
        for (Row row : r.rows) {
            values.add(row.value);
        }
        return new ScanResult(values, r.ledgersScanned, r.ledgersPrunedByDate);
    }

    /** A matched row: its message properties (indexed/extracted fields) and its value payload. */
    public static final class Row {
        public final Map<String, String> properties;
        public final byte[] value;

        Row(Map<String, String> properties, byte[] value) {
            this.properties = properties;
            this.value = value;
        }
    }

    /** Scan result carrying full rows (properties + value) plus prune stats. */
    public static final class RowResult {
        public final List<Row> rows;
        public final int ledgersScanned;
        public final int ledgersPrunedByDate;
        public final int pagesRead;        // candidate pages surviving date + bookie prune (i.e. read)
        public final int granulesTotal;    // granules across the read pages
        public final int granulesExamined; // granule zone maps inspected (sparse index narrows this)
        public final int granulesRead;     // granules whose zone map survived -> column data decoded
        public final long cellsScanned;    // column cells read during predicate eval (PREWHERE shrinks this)

        RowResult(List<Row> rows, int ledgersScanned, int ledgersPrunedByDate, int pagesRead,
                  int granulesTotal, int granulesExamined, int granulesRead, long cellsScanned) {
            this.rows = rows;
            this.ledgersScanned = ledgersScanned;
            this.ledgersPrunedByDate = ledgersPrunedByDate;
            this.pagesRead = pagesRead;
            this.granulesTotal = granulesTotal;
            this.granulesExamined = granulesExamined;
            this.granulesRead = granulesRead;
            this.cellsScanned = cellsScanned;
        }
    }

    /** Core scan returning full rows; the {@code byte[]}-row variants wrap this. */
    public static RowResult scanRows(PersistentTopic topic, BookKeeper bk, List<Bound> bounds,
                                     Map<Short, List<byte[]>> keySets, long fromDate, long toDate)
            throws Exception {
        ManagedLedger ml = topic.getManagedLedger();
        byte[] predicate = buildPredicate(bounds, keySets);
        BookieClient bookieClient = bk.getClientCtx().getBookieClient();
        // The current (still-open) ledger reports 0 entries in its LedgerInfo; use the LAC for it.
        Position lac = ml.getLastConfirmedEntry();
        Map<Long, long[]> dateIndex = topic.getStreamLakeDateIndex();
        boolean dateFilter = fromDate != Long.MIN_VALUE || toDate != Long.MAX_VALUE;

        List<Row> result = new ArrayList<>();
        int scanned = 0;
        int prunedByDate = 0;
        int pagesRead = 0;
        int granulesTotal = 0;
        int granulesExamined = 0;
        int granulesRead = 0;
        long cellsScanned = 0;
        for (Map.Entry<Long, LedgerInfo> e : ml.getLedgersInfo().entrySet()) {
            long ledgerId = e.getKey();
            long lastEntry = (lac != null && lac.getLedgerId() == ledgerId)
                    ? lac.getEntryId() : e.getValue().getEntries() - 1;
            if (lastEntry < 0) {
                continue;
            }
            // point 9: date-partition prune -- skip ledgers entirely outside the query window
            if (dateFilter) {
                long[] dr = dateIndex.get(ledgerId);
                if (dr != null && (dr[1] < fromDate || dr[0] > toDate)) {
                    prunedByDate++;
                    continue;
                }
            }
            scanned++;
            BookieId bookie = bk.getLedgerManager().readLedgerMetadata(ledgerId)
                    .get(30, TimeUnit.SECONDS).getValue().getAllEnsembles().firstEntry().getValue().get(0);
            // point 10/11: bookie prunes pages by column range AND key-set bloom
            List<Long> pages = bookieClient
                    .pagePrune(bookie, ledgerId, 0, lastEntry, predicate)
                    .get(30, TimeUnit.SECONDS);
            pagesRead += pages.size();
            for (long entryId : pages) {
                // point 12/13: read only surviving pages; inside each, prune GRANULES by their zone
                // maps (min/max + set + bloom) before reading column data / evaluating rows.
                ByteBuf pageData = readEntry(topic, ledgerId, entryId);
                try {
                    int n = StreamLakeBatchPage.messageCount(pageData);
                    int gsize = StreamLakeBatchPage.granuleSize(pageData);
                    int numG = StreamLakeBatchPage.granuleCount(pageData);
                    Map<Short, StreamLakeBatchPage.GranuleStat[]> zone =
                            StreamLakeBatchPage.decodeZoneMaps(pageData);
                    granulesTotal += numG;

                    // Feature 3 (sparse primary index): if the page is sorted by a predicate column,
                    // binary-search its per-granule marks for the candidate granule window instead of
                    // inspecting every granule's zone map.
                    int gStart = 0;
                    int gEnd = numG;
                    if (StreamLakeBatchPage.isSorted(pageData)) {
                        short sortCol = (short) StreamLakeBatchPage.sortColumnId(pageData);
                        Bound sortBound = boundOn(bounds, sortCol);
                        StreamLakeBatchPage.GranuleStat[] marks = zone.get(sortCol);
                        if (sortBound != null && marks != null) {
                            int[] window = sparseWindow(marks, sortBound);
                            gStart = window[0];
                            gEnd = window[1];
                        }
                    }

                    for (int g = gStart; g < gEnd; g++) {
                        granulesExamined++;
                        int from = g * gsize;
                        int to = Math.min(from + gsize, n);
                        if (granuleSkipped(zone, g, bounds, keySets)) {
                            continue;
                        }
                        granulesRead++;
                        // Feature 2 (PREWHERE / late materialization): evaluate the most selective
                        // predicate column first; read each later column ONLY at the rows that still
                        // survive, so unselective columns are barely touched.
                        int[] survivors = new int[to - from];
                        for (int i = from; i < to; i++) {
                            survivors[i - from] = i;
                        }
                        int survivorCount = survivors.length;
                        for (Bound b : orderBySelectivity(bounds, zone, g)) {
                            if (survivorCount == 0) {
                                break;
                            }
                            long[] vals = StreamLakeBatchPage.readColumnAt(pageData, b.columnId,
                                    survivors, survivorCount);
                            cellsScanned += survivorCount;
                            int w = 0;
                            for (int i = 0; i < survivorCount; i++) {
                                if (passes(b, vals[i])) {
                                    survivors[w++] = survivors[i];
                                }
                            }
                            survivorCount = w;
                        }
                        for (int i = 0; i < survivorCount; i++) {
                            ByteBuf msg = StreamLakeBatchPage.messageAt(pageData, survivors[i]);
                            try {
                                result.add(extractRow(msg));
                            } finally {
                                msg.release();
                            }
                        }
                    }
                } finally {
                    pageData.release();
                }
            }
        }
        return new RowResult(result, scanned, prunedByDate, pagesRead,
                granulesTotal, granulesExamined, granulesRead, cellsScanned);
    }

    /** First bound on {@code columnId}, or null. */
    private static Bound boundOn(List<Bound> bounds, short columnId) {
        for (Bound b : bounds) {
            if (b.columnId == columnId) {
                return b;
            }
        }
        return null;
    }

    /** True if a granule's zone map proves it cannot contain a row matching the predicate. */
    private static boolean granuleSkipped(Map<Short, StreamLakeBatchPage.GranuleStat[]> zone, int g,
                                          List<Bound> bounds, Map<Short, List<byte[]>> keySets) {
        for (Bound b : bounds) {
            StreamLakeBatchPage.GranuleStat[] stats = zone.get(b.columnId);
            if (stats == null) {
                continue;
            }
            StreamLakeBatchPage.GranuleStat s = stats[g];
            if (b.gt != null && s.max <= b.gt) {
                return true; // every value <= gt -> none satisfies value > gt
            }
            if (b.lt != null && s.min >= b.lt) {
                return true; // every value >= lt -> none satisfies value < lt
            }
            if (b.eq != null) {
                if (b.eq < s.min || b.eq > s.max) {
                    return true; // eq outside [min,max]
                }
                if (s.set != null && !s.setContains(b.eq)) {
                    return true; // exact set proves eq absent even though it is inside [min,max]
                }
            }
        }
        if (keySets != null) {
            for (Map.Entry<Short, List<byte[]>> e : keySets.entrySet()) {
                StreamLakeBatchPage.GranuleStat[] stats = zone.get(e.getKey());
                if (stats == null || stats[g].bloom == null) {
                    continue;
                }
                boolean anyHit = false;
                for (byte[] key : e.getValue()) {
                    if (BloomFilter.mightContain(stats[g].bloom, key)) {
                        anyHit = true;
                        break;
                    }
                }
                if (!anyHit) {
                    return true; // granule bloom holds none of the probe keys
                }
            }
        }
        return false;
    }

    /** True if a single value satisfies the bound (the exact per-row predicate). */
    private static boolean passes(Bound b, long v) {
        if (b.eq != null && v != b.eq) {
            return false;
        }
        if (b.gt != null && !(v > b.gt)) {
            return false;
        }
        if (b.lt != null && !(v < b.lt)) {
            return false;
        }
        return true;
    }

    /** Bounds ordered most-selective-first for this granule (PREWHERE column ordering). */
    private static List<Bound> orderBySelectivity(List<Bound> bounds,
            Map<Short, StreamLakeBatchPage.GranuleStat[]> zone, int g) {
        if (bounds.size() < 2) {
            return bounds;
        }
        List<Bound> ordered = new ArrayList<>(bounds);
        ordered.sort(java.util.Comparator.comparingDouble(b -> {
            StreamLakeBatchPage.GranuleStat[] stats = zone.get(b.columnId);
            return selectivity(b, stats == null ? null : stats[g]);
        }));
        return ordered;
    }

    /** Estimated surviving fraction of a granule's rows for {@code b} (0 = most selective). */
    private static double selectivity(Bound b, StreamLakeBatchPage.GranuleStat s) {
        if (s == null) {
            return 1.0;
        }
        double span = (double) s.max - (double) s.min;
        if (b.eq != null) {
            if (s.set != null) {
                return s.setContains(b.eq) ? 1.0 / s.set.length : 0.0;
            }
            if (b.eq < s.min || b.eq > s.max) {
                return 0.0;
            }
            return span <= 0 ? 1.0 : 1.0 / (span + 1.0);
        }
        double frac = 1.0;
        if (b.gt != null) {
            if (s.max <= b.gt) {
                return 0.0;
            }
            frac = span <= 0 ? 1.0 : Math.min(1.0, ((double) s.max - b.gt) / span);
        }
        if (b.lt != null) {
            if (s.min >= b.lt) {
                return 0.0;
            }
            frac = Math.min(frac, span <= 0 ? 1.0 : Math.min(1.0, ((double) b.lt - s.min) / span));
        }
        return frac;
    }

    /**
     * Candidate granule window {@code [start, end)} for a bound on the page's sort column, found by
     * binary-searching the per-granule marks (mins and maxs are non-decreasing on a sorted page).
     */
    private static int[] sparseWindow(StreamLakeBatchPage.GranuleStat[] marks, Bound b) {
        int numG = marks.length;
        int gStart = 0;
        int gEnd = numG;
        if (b.eq != null) {
            gStart = firstMaxGe(marks, b.eq);   // granules whose max < eq cannot hold it
            gEnd = firstMinGt(marks, b.eq);     // granules whose min > eq cannot hold it
        } else {
            if (b.gt != null) {
                gStart = firstMaxGt(marks, b.gt); // value > gt lives in the suffix
            }
            if (b.lt != null) {
                gEnd = firstMinGe(marks, b.lt);   // value < lt lives in the prefix
            }
        }
        if (gStart > gEnd) {
            gStart = gEnd;
        }
        return new int[]{gStart, gEnd};
    }

    private static int firstMaxGe(StreamLakeBatchPage.GranuleStat[] m, long x) {
        int lo = 0;
        int hi = m.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (m[mid].max >= x) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    private static int firstMaxGt(StreamLakeBatchPage.GranuleStat[] m, long x) {
        int lo = 0;
        int hi = m.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (m[mid].max > x) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    private static int firstMinGe(StreamLakeBatchPage.GranuleStat[] m, long x) {
        int lo = 0;
        int hi = m.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (m[mid].min >= x) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    private static int firstMinGt(StreamLakeBatchPage.GranuleStat[] m, long x) {
        int lo = 0;
        int hi = m.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (m[mid].min > x) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    private static Row extractRow(ByteBuf msg) {
        MessageMetadata md = Commands.parseMessageMetadata(msg); // advances msg to the payload
        Map<String, String> props = new HashMap<>();
        for (int i = 0; i < md.getPropertiesCount(); i++) {
            props.put(md.getPropertyAt(i).getKey(), md.getPropertyAt(i).getValue());
        }
        byte[] value = new byte[msg.readableBytes()];
        msg.getBytes(msg.readerIndex(), value);
        return new Row(props, value);
    }

    private static byte[] buildPredicate(List<Bound> bounds, Map<Short, List<byte[]>> keySets) {
        Map<Short, List<PageRangeCodec.Range>> pred = new HashMap<>();
        for (Bound b : bounds) {
            PageRangeCodec.Range range;
            if (b.eq != null) {
                // equality is an inclusive point range [eq, eq]; the page-level min/max prune keeps a
                // page iff eq is within its [min,max] (the exact set then prunes granules within it).
                byte[] point = encInt(b.eq);
                range = new PageRangeCodec.Range(point, point, false, false);
            } else {
                byte[] min = b.gt != null ? encInt(b.gt) : null;
                byte[] max = b.lt != null ? encInt(b.lt) : null;
                range = new PageRangeCodec.Range(min, max, b.gt != null, b.lt != null);
            }
            pred.computeIfAbsent(b.columnId, k -> new ArrayList<>()).add(range);
        }
        return PageRangeCodec.encodePredicate(pred, keySets == null ? new HashMap<>() : keySets);
    }

    /** Order-preserving encoding for an int key (matches the page-side column encoding). */
    public static byte[] encodeKey(int v) {
        return encInt(v);
    }

    private static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    private static ByteBuf readEntry(PersistentTopic topic, long ledgerId, long entryId)
            throws Exception {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();
        topic.asyncReadEntry(PositionFactory.create(ledgerId, entryId), new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                ByteBuf copy = entry.getDataBuffer().retainedDuplicate();
                entry.release();
                future.complete(copy);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future.get(30, TimeUnit.SECONDS);
    }

    /** Convenience for a single departmentId-style greater-than predicate. */
    public static List<Bound> gt(short columnId, String columnName, int value) {
        return Collections.singletonList(new Bound(columnId, columnName, value, null));
    }
}
