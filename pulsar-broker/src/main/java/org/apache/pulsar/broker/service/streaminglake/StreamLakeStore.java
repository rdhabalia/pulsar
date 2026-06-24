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

import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieClient;

/**
 * Column-major storage for a StreamLake topic, built directly on BookKeeper.
 *
 * <p>Write path: rows are buffered and grouped into a page once the page reaches a target
 * size (~2&nbsp;MB) or the grouping window (a few ms) elapses. Each page is encoded
 * column-major ({@link ColumnarPage}), and the broker derives per-column min/max ranges
 * from the topic schema and ships them to the bookie inside {@code addEntry} (via
 * {@link LedgerHandle#asyncAddEntry(io.netty.buffer.ByteBuf, byte[],
 * org.apache.bookkeeper.client.AsyncCallback.AddCallback, Object)}), where they land in the
 * ledger-page-index.
 *
 * <p>Consumer read path: {@link #readPage} reads a page from the bookie and decodes it back
 * into rows (the broker would re-wrap these as Pulsar messages for a normal consumer).
 *
 * <p>StreamLake scan path: {@link #scan} prunes by the date-partition column in the broker,
 * pushes the remaining predicate (e.g. departmentId&nbsp;&gt;&nbsp;X) to the bookie via
 * {@link BookieClient#pagePrune}, and only the surviving pages are read, decoded and filtered.
 */
public class StreamLakeStore implements AutoCloseable {

    private static final byte[] PASSWORD = "streamlake".getBytes();

    private final BookKeeper bk;
    private final String[] colNames;
    private final byte[] colTypes;
    private final int eventTimeCol;     // index of the LONG date-partition column
    private final short[] indexedCols;  // INT columns shipped to the bookie as min/max ranges
    private final int maxPageBytes;
    private final int maxPageRows;
    private final long maxPageDelayMs;

    private LedgerHandle writeLedger;
    private LedgerHandle readLedger;
    private final List<Object[]> buffer = new ArrayList<>();
    private int bufferBytes;
    private long bufferOpenedAt;
    private final List<PageMeta> pages = new ArrayList<>();

    // observability for the last scan(): how aggressively each prune level cut pages.
    public int lastDateCandidatePages;
    public int lastBookieKeptPages;

    /** Directory entry for one sealed page: where it is, and its date-partition bounds. */
    public static final class PageMeta {
        public final long ledgerId;
        public final long entryId;
        public final long minEventTime;
        public final long maxEventTime;

        PageMeta(long ledgerId, long entryId, long minEventTime, long maxEventTime) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.minEventTime = minEventTime;
            this.maxEventTime = maxEventTime;
        }
    }

    /** A predicate on one INT column: exclusive lower ({@code gt}) and/or upper ({@code lt}). */
    public static final class Bound {
        public final int col;
        public final Integer gt;
        public final Integer lt;

        public Bound(int col, Integer gt, Integer lt) {
            this.col = col;
            this.gt = gt;
            this.lt = lt;
        }
    }

    public StreamLakeStore(BookKeeper bk, String[] colNames, byte[] colTypes, int eventTimeCol,
                           short[] indexedCols, int maxPageBytes, int maxPageRows, long maxPageDelayMs) {
        this.bk = bk;
        this.colNames = colNames;
        this.colTypes = colTypes;
        this.eventTimeCol = eventTimeCol;
        this.indexedCols = indexedCols;
        this.maxPageBytes = maxPageBytes;
        this.maxPageRows = maxPageRows;
        this.maxPageDelayMs = maxPageDelayMs;
    }

    // ---- write path -------------------------------------------------------

    public synchronized void append(Object[] row) throws Exception {
        if (writeLedger == null) {
            writeLedger = bk.createLedger(1, 1, BookKeeper.DigestType.CRC32, PASSWORD);
        }
        if (buffer.isEmpty()) {
            bufferOpenedAt = System.currentTimeMillis();
        }
        buffer.add(row);
        bufferBytes += estimateRowBytes(row);
        boolean full = bufferBytes >= maxPageBytes || buffer.size() >= maxPageRows;
        boolean windowElapsed = (System.currentTimeMillis() - bufferOpenedAt) >= maxPageDelayMs;
        if (full || windowElapsed) {
            sealPage();
        }
    }

    /** Flush whatever is buffered into a final page. */
    public synchronized void flush() throws Exception {
        if (!buffer.isEmpty()) {
            sealPage();
        }
    }

    private void sealPage() throws Exception {
        List<Object[]> rows = new ArrayList<>(buffer);
        byte[] payload = ColumnarPage.encode(colNames, colTypes, rows);
        byte[] rangeBlob = buildPageRangeBlob(rows);

        long minT = Long.MAX_VALUE;
        long maxT = Long.MIN_VALUE;
        for (Object[] row : rows) {
            long t = ((Number) row[eventTimeCol]).longValue();
            minT = Math.min(minT, t);
            maxT = Math.max(maxT, t);
        }

        CompletableFuture<Long> done = new CompletableFuture<>();
        writeLedger.asyncAddEntry(Unpooled.wrappedBuffer(payload), rangeBlob,
                (rc, lh, entryId, ctx) -> {
                    if (rc == BKException.Code.OK) {
                        done.complete(entryId);
                    } else {
                        done.completeExceptionally(BKException.create(rc));
                    }
                }, null);
        long entryId = done.get(60, TimeUnit.SECONDS);
        pages.add(new PageMeta(writeLedger.getId(), entryId, minT, maxT));

        buffer.clear();
        bufferBytes = 0;
    }

    /** Derive the per-column min/max blob the bookie stores in its page index. */
    private byte[] buildPageRangeBlob(List<Object[]> rows) {
        Map<Short, PageRangeCodec.Range> cols = new HashMap<>();
        for (short ci : indexedCols) {
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            for (Object[] row : rows) {
                int v = ((Number) row[ci]).intValue();
                min = Math.min(min, v);
                max = Math.max(max, v);
            }
            cols.put(ci, new PageRangeCodec.Range(encInt(min), encInt(max), false, false));
        }
        return PageRangeCodec.encodePage(cols);
    }

    /** Seal the buffer and close the writable ledger so the pages can be read back. */
    public synchronized void seal() throws Exception {
        flush();
        if (writeLedger != null) {
            writeLedger.close();
        }
    }

    // ---- consumer read path ----------------------------------------------

    /** Read a page from the bookie and decode it back into rows (consumer materialization). */
    public synchronized List<Object[]> readPage(long ledgerId, long entryId) throws Exception {
        LedgerHandle lh = readHandle(ledgerId);
        Enumeration<LedgerEntry> en = lh.readEntries(entryId, entryId);
        byte[] data = en.nextElement().getEntry();
        return ColumnarPage.decode(data).rows;
    }

    /** Materialize every row of the topic, page by page (a full-scan consumer). */
    public synchronized List<Object[]> readAll() throws Exception {
        List<Object[]> all = new ArrayList<>();
        for (PageMeta pm : pages) {
            all.addAll(readPage(pm.ledgerId, pm.entryId));
        }
        return all;
    }

    // ---- StreamLake scan path --------------------------------------------

    /**
     * Three-level pruning: (1) date-partition prune in the broker, (2) bookie
     * {@code PAGE_PRUNE} on the column ranges, (3) exact row filter on the decoded survivors.
     */
    public synchronized List<Object[]> scan(long fromTime, long toTime, List<Bound> bounds)
            throws Exception {
        // 1. date-partition prune (broker) -> candidate pages per ledger
        Map<Long, List<Long>> dateCandidates = new HashMap<>();
        for (PageMeta pm : pages) {
            if (pm.maxEventTime < fromTime || pm.minEventTime > toTime) {
                continue;
            }
            dateCandidates.computeIfAbsent(pm.ledgerId, k -> new ArrayList<>()).add(pm.entryId);
        }
        lastDateCandidatePages = dateCandidates.values().stream().mapToInt(List::size).sum();
        lastBookieKeptPages = 0;
        if (dateCandidates.isEmpty()) {
            return new ArrayList<>();
        }

        // 2. push the remaining predicate to the bookie (PAGE_PRUNE)
        byte[] predicateBlob = buildPredicateBlob(bounds);
        BookieClient bookieClient = bk.getClientCtx().getBookieClient();

        List<Object[]> result = new ArrayList<>();
        for (Map.Entry<Long, List<Long>> e : dateCandidates.entrySet()) {
            long ledgerId = e.getKey();
            List<Long> dateKept = e.getValue();
            long minEntry = dateKept.stream().mapToLong(Long::longValue).min().getAsLong();
            long maxEntry = dateKept.stream().mapToLong(Long::longValue).max().getAsLong();

            BookieId bookie = readHandle(ledgerId).getLedgerMetadata()
                    .getAllEnsembles().get(0L).get(0);
            List<Long> bookieKept = bookieClient
                    .pagePrune(bookie, ledgerId, minEntry, maxEntry, predicateBlob)
                    .get(60, TimeUnit.SECONDS);

            // 3. only pages surviving both date prune AND bookie prune are read + filtered
            for (Long entryId : bookieKept) {
                if (!dateKept.contains(entryId)) {
                    continue;
                }
                lastBookieKeptPages++;
                for (Object[] row : readPage(ledgerId, entryId)) {
                    if (rowMatches(row, fromTime, toTime, bounds)) {
                        result.add(row);
                    }
                }
            }
        }
        return result;
    }

    private byte[] buildPredicateBlob(List<Bound> bounds) {
        Map<Short, List<PageRangeCodec.Range>> pred = new HashMap<>();
        for (Bound b : bounds) {
            byte[] min = b.gt != null ? encInt(b.gt) : null;
            byte[] max = b.lt != null ? encInt(b.lt) : null;
            pred.computeIfAbsent((short) b.col, k -> new ArrayList<>())
                    .add(new PageRangeCodec.Range(min, max, b.gt != null, b.lt != null));
        }
        return PageRangeCodec.encode(pred);
    }

    private boolean rowMatches(Object[] row, long fromTime, long toTime, List<Bound> bounds) {
        long t = ((Number) row[eventTimeCol]).longValue();
        if (t < fromTime || t > toTime) {
            return false;
        }
        for (Bound b : bounds) {
            int v = ((Number) row[b.col]).intValue();
            if (b.gt != null && !(v > b.gt)) {
                return false;
            }
            if (b.lt != null && !(v < b.lt)) {
                return false;
            }
        }
        return true;
    }

    public List<PageMeta> pages() {
        return pages;
    }

    // ---- helpers ----------------------------------------------------------

    private LedgerHandle readHandle(long ledgerId) throws Exception {
        if (readLedger != null && readLedger.getId() == ledgerId) {
            return readLedger;
        }
        readLedger = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32, PASSWORD);
        return readLedger;
    }

    private int estimateRowBytes(Object[] row) {
        int bytes = 0;
        for (int c = 0; c < colTypes.length; c++) {
            switch (colTypes[c]) {
                case ColumnarPage.INT:
                    bytes += 4;
                    break;
                case ColumnarPage.LONG:
                    bytes += 8;
                    break;
                case ColumnarPage.STRING:
                    bytes += 4 + ((String) row[c]).length();
                    break;
                default:
                    break;
            }
        }
        return bytes;
    }

    /** Order-preserving big-endian encoding of a signed int for unsigned-byte comparison. */
    private static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    @Override
    public synchronized void close() throws Exception {
        if (readLedger != null) {
            readLedger.close();
            readLedger = null;
        }
        if (writeLedger != null && !writeLedger.isClosed()) {
            writeLedger.close();
        }
    }
}
