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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The shared, per-topic <b>page-index ledger</b> (redesign phase C): the durable home for the
 * per-batch stats footers the client emits. On each StreamLake batch the broker slices the message's
 * stats footer (without parsing Arrow) and appends it here, keyed by the data-ledger entry it
 * describes. A scan reads this instead of the 1&nbsp;MB data pages to prune, and the bookie performs
 * no predicate work.
 *
 * <p>One <b>chain</b> of BookKeeper ledgers per topic (not one ledger per data ledger) whose ids live
 * in the {@code /streamlake} node via {@link StreamLakeMetaStore#setPageIndexLedgerIds}. The head
 * rolls when it reaches {@code maxHeadBytes} or is fenced; a data ledger's footers may therefore span
 * a roll. Entry format: {@code 'F' | dataLedgerId(8) | dataEntryId(8) | footer bytes}.
 *
 * <p>Memory-light: only lightweight references ({@code dataLedgerId -> [(piLedgerId, piEntryId,
 * dataEntryId)]}) are held; the footer bytes stay in the ledger and are read on demand by
 * {@link #footersFor}. On {@link #open} the chain is replayed to rebuild those references (footer
 * bodies are skipped). Correctness never depends on the index being current -- a scan falls back to
 * the message's own footer for entries not yet appended here.
 */
public class StreamLakePageIndex implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamLakePageIndex.class);
    private static final byte[] PASSWORD = "streamlake-page".getBytes();
    private static final byte ENTRY_FOOTER = (byte) 'F';
    private static final int HEADER = 1 + 8 + 8; // type + dataLedgerId + dataEntryId
    private static final long DEFAULT_MAX_HEAD_BYTES = 4L * 1024 * 1024;
    private static final int DEFAULT_MAX_ENTRIES = 1_000_000;
    // Max page-index entries pulled per readEntries() during replay. Bounds in-flight bookie reads so a
    // large ledger's replay cannot trip the bookie's "too many read requests" flow control (-> timeout).
    private static final int REPLAY_READ_BATCH = 500;

    /** One stored footer: the data-ledger entry it describes, and its stats-footer bytes. */
    public static final class PageFooter {
        public final long dataEntryId;
        public final byte[] stats;

        PageFooter(long dataEntryId, byte[] stats) {
            this.dataEntryId = dataEntryId;
            this.stats = stats;
        }
    }

    /** A lightweight pointer to a footer entry in the chain (no footer bytes held in memory). */
    private static final class Ref {
        final long piLedgerId;
        final long piEntryId;
        final long dataEntryId;

        Ref(long piLedgerId, long piEntryId, long dataEntryId) {
            this.piLedgerId = piLedgerId;
            this.piEntryId = piEntryId;
            this.dataEntryId = dataEntryId;
        }
    }

    private final BookKeeper bk;
    private final StreamLakeMetaStore metaStore;
    private final long maxHeadBytes;
    private final int maxEntriesPerLedger;
    private final int ensembleSize;
    private final int writeQuorum;
    private final int ackQuorum;

    private final Map<Long, List<Ref>> refsByDataLedger = new HashMap<>();
    private final List<Long> chain = new ArrayList<>();
    private final Map<Long, LedgerHandle> readHandles = new HashMap<>();
    private LedgerHandle head;
    private long headBytes;
    private int headEntryCount;
    private long headDataLedger = -1; // the data ledger the head is currently accumulating footers for

    // ---- async append state (streamLakePageIndexAsyncAppend), all mutated under `this` ----
    // A roll (createLedger) must not run while adds to the old head are still in flight, so a roll
    // drains: `rolling` fences new appends into `deferredAppends`, and the last in-flight completion
    // starts the async roll. `inFlightAdds` counts asyncAddEntry ops submitted to the current head but
    // not yet acked. No I/O is ever performed while holding `this`.
    private boolean rolling;
    private long inFlightAdds;
    private final Deque<PendingAppend> deferredAppends = new ArrayDeque<>();

    /** An append deferred while a head roll drains/completes; replayed onto the fresh head. */
    private static final class PendingAppend {
        final long dataLedgerId;
        final long dataEntryId;
        final byte[] footer;
        final CompletableFuture<Void> result;

        PendingAppend(long dataLedgerId, long dataEntryId, byte[] footer, CompletableFuture<Void> result) {
            this.dataLedgerId = dataLedgerId;
            this.dataEntryId = dataEntryId;
            this.footer = footer;
            this.result = result;
        }
    }

    private StreamLakePageIndex(BookKeeper bk, StreamLakeMetaStore metaStore,
                                long maxHeadBytes, int maxEntriesPerLedger,
                                int ensembleSize, int writeQuorum, int ackQuorum) {
        this.bk = bk;
        this.metaStore = metaStore;
        this.maxHeadBytes = maxHeadBytes > 0 ? maxHeadBytes : DEFAULT_MAX_HEAD_BYTES;
        this.maxEntriesPerLedger = maxEntriesPerLedger > 0 ? maxEntriesPerLedger : DEFAULT_MAX_ENTRIES;
        this.ensembleSize = ensembleSize;
        this.writeQuorum = writeQuorum;
        this.ackQuorum = ackQuorum;
    }

    /** Load and replay the page-index ledger chain; never throws (falls back to an empty index). */
    public static StreamLakePageIndex open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore) {
        return open(bk, ml, metaStore, DEFAULT_MAX_HEAD_BYTES);
    }

    public static StreamLakePageIndex open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                           long maxHeadBytes) {
        return open(bk, ml, metaStore, maxHeadBytes, 1, 1, 1);
    }

    /**
     * Open with an explicit replication but the default head size. The page index is the hot pruning
     * metadata, so production runs a high {@code ensembleSize} (many bookies for read scaling) with a
     * smaller {@code ackQuorum} (write to many, wait for a few) on the isolated metadata bookie pool.
     */
    public static StreamLakePageIndex open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
            int ensembleSize, int writeQuorum, int ackQuorum) {
        return open(bk, ml, metaStore, DEFAULT_MAX_HEAD_BYTES, ensembleSize, writeQuorum, ackQuorum);
    }

    /** Open with an explicit replication (production: RF-3 on the isolated metadata bookie pool). */
    public static StreamLakePageIndex open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
            long maxHeadBytes, int ensembleSize, int writeQuorum, int ackQuorum) {
        return open(bk, ml, metaStore, maxHeadBytes, DEFAULT_MAX_ENTRIES, ensembleSize, writeQuorum, ackQuorum);
    }

    /**
     * Open with an explicit replication and a max-entries-per-ledger roll threshold. One page-index
     * ledger holds many whole data ledgers; it rolls at a data-ledger boundary once it crosses
     * {@code maxEntriesPerLedger} (the byte cap is a hard safety guard), so each data ledger's footers
     * stay contiguous in one ledger.
     */
    public static StreamLakePageIndex open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
            long maxHeadBytes, int maxEntriesPerLedger, int ensembleSize, int writeQuorum, int ackQuorum) {
        return open(bk, metaStore, maxHeadBytes, maxEntriesPerLedger, ensembleSize, writeQuorum, ackQuorum);
    }

    /** Headless open by metastore alone (no ManagedLedger) -- for off-broker builds. */
    public static StreamLakePageIndex open(BookKeeper bk, StreamLakeMetaStore metaStore,
            long maxHeadBytes, int maxEntriesPerLedger, int ensembleSize, int writeQuorum, int ackQuorum) {
        StreamLakePageIndex idx = new StreamLakePageIndex(bk, metaStore, maxHeadBytes,
                maxEntriesPerLedger, ensembleSize, writeQuorum, ackQuorum);
        try {
            idx.chain.addAll(metaStore.read().pageIndexLedgerIds);
            idx.replay();
        } catch (Exception e) {
            log.warn("StreamLake page index falling back to empty for {}: {}",
                    metaStore.name(), e.toString());
        }
        return idx;
    }

    /**
     * Blocking convenience wrapper over {@link #appendFooterAsync}: append the footer and wait for the
     * bookie ack. Retained for tests and callers wanting synchronous semantics; production ingest calls
     * {@link #appendFooterAsync} directly so no broker thread ever parks on the write.
     */
    public void appendFooter(long dataLedgerId, long dataEntryId, byte[] footer) throws Exception {
        try {
            appendFooterAsync(dataLedgerId, dataEntryId, footer).get();
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    /**
     * Non-blocking append: durably append the footer via {@code asyncAddEntry} and complete the returned
     * future when the bookie acks -- no thread parks on the write, so many appends pipeline per topic.
     * Ordering is preserved (the caller submits in data-entry order and a single-writer ledger acks in
     * order), and the head roll is a drain barrier ({@code rolling} fences new appends until in-flight
     * adds complete, then an async close+create installs the fresh head). All state is mutated under
     * {@code this}; the only work under the lock is in-memory (the BK ops are async).
     */
    public CompletableFuture<Void> appendFooterAsync(long dataLedgerId, long dataEntryId, byte[] footer) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            synchronized (this) {
                submitOrDefer(new PendingAppend(dataLedgerId, dataEntryId, footer, result));
            }
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
        return result;
    }

    // Caller holds `this`. Either submit the append to the current head, or (if a roll is needed or in
    // progress) queue it and kick/await the roll.
    private void submitOrDefer(PendingAppend a) {
        if (rolling) {
            deferredAppends.add(a);
            return;
        }
        byte[] entry = encode(a.dataLedgerId, a.dataEntryId, a.footer);
        boolean boundary = head != null && headDataLedger != a.dataLedgerId;
        boolean overEntries = head != null && headEntryCount >= maxEntriesPerLedger;
        boolean overBytes = head != null && headBytes > 0 && headBytes + entry.length > maxHeadBytes;
        boolean needRoll = head == null || (boundary && overEntries) || overBytes;
        if (needRoll) {
            rolling = true;
            deferredAppends.add(a);
            if (inFlightAdds == 0) {
                startRoll();            // nothing in flight to the old head -> roll immediately
            }                           // else: the last in-flight completion starts the roll
            return;
        }
        headEntryCount++;
        headBytes += entry.length;
        headDataLedger = a.dataLedgerId;
        inFlightAdds++;
        head.asyncAddEntry(entry, (rc, lh, entryId, ctx) ->
                onAddComplete(rc, lh, entryId, a), null);
    }

    // BK add-callback thread. Record the ref (in order -- a single-writer ledger acks in add order),
    // complete the publish, and if a roll was waiting for this ledger to drain, start it now.
    private void onAddComplete(int rc, LedgerHandle lh, long entryId, PendingAppend a) {
        boolean startRollNow = false;
        synchronized (this) {
            inFlightAdds--;
            if (rc == BKException.Code.OK) {
                refsByDataLedger.computeIfAbsent(a.dataLedgerId, k -> new ArrayList<>())
                        .add(new Ref(lh.getId(), entryId, a.dataEntryId));
            }
            if (rolling && inFlightAdds == 0) {
                startRollNow = true;
            }
        }
        if (rc == BKException.Code.OK) {
            a.result.complete(null);
        } else {
            a.result.completeExceptionally(BKException.create(rc));
        }
        if (startRollNow) {
            synchronized (this) {
                startRoll();
            }
        }
    }

    // Caller holds `this`, `inFlightAdds == 0`, `rolling == true`. Async close the old head, create a
    // fresh one, persist the chain, then replay the deferred appends onto it. No blocking under `this`.
    private void startRoll() {
        LedgerHandle old = head;
        AsyncCallback.CreateCallback onCreated = (rc, fresh, ctx) -> {
            List<PendingAppend> replay = null;
            RuntimeException failure = null;
            synchronized (this) {
                if (rc == BKException.Code.OK && fresh != null) {
                    chain.add(fresh.getId());
                    try {
                        metaStore.setPageIndexLedgerIds(chain);   // rare (per roll) metadata write
                    } catch (Exception e) {
                        log.warn("StreamLake page-index chain persist failed for {}: {}",
                                metaStore.name(), e.toString());
                    }
                    head = fresh;
                    headBytes = 0;
                    headEntryCount = 0;
                    rolling = false;
                    replay = new ArrayList<>(deferredAppends);
                    deferredAppends.clear();
                } else {
                    failure = new RuntimeException("StreamLake page-index roll (createLedger) failed rc=" + rc);
                }
            }
            if (failure != null) {
                failDeferred(failure);
                return;
            }
            // Replay deferred appends onto the fresh head, in order, each under `this`.
            for (PendingAppend a : replay) {
                synchronized (this) {
                    submitOrDefer(a);
                }
            }
        };
        AsyncCallback.CloseCallback onClosed = (rc, lh, ctx) ->
                bk.asyncCreateLedger(ensembleSize, writeQuorum, ackQuorum,
                        BookKeeper.DigestType.CRC32, PASSWORD, onCreated, null, Collections.emptyMap());
        if (old != null) {
            old.asyncClose(onClosed, null);
        } else {
            bk.asyncCreateLedger(ensembleSize, writeQuorum, ackQuorum,
                    BookKeeper.DigestType.CRC32, PASSWORD, onCreated, null, Collections.emptyMap());
        }
    }

    // Fail every queued append (roll could not produce a head). Clears `rolling` so the topic can retry.
    private void failDeferred(Throwable cause) {
        List<PendingAppend> failed;
        synchronized (this) {
            rolling = false;
            failed = new ArrayList<>(deferredAppends);
            deferredAppends.clear();
        }
        for (PendingAppend a : failed) {
            a.result.completeExceptionally(cause);
        }
    }

    /** The stored footers for a data ledger, ordered by data-entry, read on demand from the chain. */
    public List<PageFooter> footersFor(long dataLedgerId) throws Exception {
        List<Ref> refs;
        synchronized (this) {
            List<Ref> src = refsByDataLedger.get(dataLedgerId);
            if (src == null || src.isEmpty()) {
                return Collections.emptyList();
            }
            refs = new ArrayList<>(src);   // snapshot under the lock; do the BK reads outside it
        }
        List<PageFooter> out = new ArrayList<>(refs.size());
        for (Ref ref : refs) {
            byte[] data = readEntry(ref.piLedgerId, ref.piEntryId);
            out.add(new PageFooter(ref.dataEntryId, footerBody(data)));
        }
        return out;
    }

    /**
     * Read a contiguous footer range (a segmented data ledger's page‑index range from its catalog
     * offset) directly from BookKeeper, without resident refs — the on‑demand precise tier used to
     * recover exact set(N)/collapsed‑column precision at query time. Returns {@code [(dataEntryId,
     * footerBytes)]} in entry order.
     */
    public List<PageFooter> readRange(long piLedgerId, long startEntry, long endEntry)
            throws Exception {
        if (piLedgerId < 0 || startEntry < 0 || endEntry < startEntry) {
            return Collections.emptyList();
        }
        LedgerHandle lh = handleFor(piLedgerId);   // opens outside the lock if needed
        List<PageFooter> out = new ArrayList<>();
        // Bounded batches: a big data ledger's page-index range can be hundreds of thousands of footers;
        // a single readEntries(start,end) would issue them all at once and overwhelm the bookie ("too
        // many read requests" -> operation timeout), which fails the whole query. Cap in-flight reads.
        for (long from = startEntry; from <= endEntry; from += REPLAY_READ_BATCH) {
            long to = Math.min(from + REPLAY_READ_BATCH - 1, endEntry);
            Enumeration<LedgerEntry> en = lh.readEntries(from, to);
            while (en.hasMoreElements()) {
                byte[] data = en.nextElement().getEntry();
                if (data.length < HEADER || data[0] != ENTRY_FOOTER) {
                    continue;
                }
                ByteBuffer bb = ByteBuffer.wrap(data);
                bb.get();           // type
                bb.getLong();       // dataLedgerId
                long dataEntryId = bb.getLong();
                out.add(new PageFooter(dataEntryId, footerBody(data)));
            }
        }
        return out;
    }

    // Get (opening + caching on first use) a read handle for a page-index ledger. Only the cache
    // lookup / head check hold `this`; the openLedger I/O runs OUTSIDE the lock so a slow open never
    // blocks the async append-completion path (which also needs `this`). The current head is read via
    // its own write handle -- never openLedger'd, which would fence our own writer.
    private LedgerHandle handleFor(long ledgerId) throws Exception {
        synchronized (this) {
            if (head != null && head.getId() == ledgerId) {
                return head;
            }
            LedgerHandle cached = readHandles.get(ledgerId);
            if (cached != null) {
                return cached;
            }
        }
        LedgerHandle opened = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32, PASSWORD);
        synchronized (this) {
            LedgerHandle existing = readHandles.get(ledgerId);
            if (existing != null) {
                try {
                    opened.close();   // lost the open race; use the cached handle
                } catch (Exception ignore) {
                    // best-effort
                }
                return existing;
            }
            readHandles.put(ledgerId, opened);
            return opened;
        }
    }

    /** Whether any footer has been recorded for a data ledger. */
    public synchronized boolean covers(long dataLedgerId) {
        List<Ref> refs = refsByDataLedger.get(dataLedgerId);
        return refs != null && !refs.isEmpty();
    }

    /** Number of data ledgers with resident footer refs (observability/tests). */
    public synchronized int residentRefLedgerCount() {
        return refsByDataLedger.size();
    }

    /**
     * The contiguous page-index entry range for a data ledger as {@code [piLedgerId, startEntry,
     * endEntry]}, so a query can seek its exact footers on demand. Returns {@code [-1,-1,-1]} when the
     * data ledger has no footers or its footers span more than one page-index ledger (then the caller
     * falls back to {@link #footersFor}).
     */
    public synchronized long[] getFooterRange(long dataLedgerId) {
        List<Ref> refs = refsByDataLedger.get(dataLedgerId);
        if (refs == null || refs.isEmpty()) {
            return new long[]{-1, -1, -1};
        }
        Ref first = refs.get(0);
        Ref last = refs.get(refs.size() - 1);
        if (first.piLedgerId != last.piLedgerId) {
            return new long[]{-1, -1, -1};
        }
        return new long[]{first.piLedgerId, first.piEntryId, last.piEntryId};
    }

    /**
     * Drop the resident footer refs for a data ledger once it is segmented (its footers are now reached
     * via the segment's stored page-index range), so resident refs stay bounded to open ledgers.
     */
    public synchronized void releaseRefs(long dataLedgerId) {
        refsByDataLedger.remove(dataLedgerId);
    }

    /** Force the current head to roll (e.g. on data-ledger close), so the next append opens a fresh one. */
    public synchronized void rollHead() throws Exception {
        if (head != null) {
            closeHeadQuietly();
            head = null;
            headBytes = 0;
        }
    }

    @Override
    public synchronized void close() {
        closeHeadQuietly();
        head = null;
        for (LedgerHandle lh : readHandles.values()) {
            try {
                lh.close();
            } catch (Exception ignore) {
                // best-effort
            }
        }
        readHandles.clear();
    }

    // ---------------------------------------------------------------- replay

    private void replay() {
        for (long ledgerId : chain) {
            try {
                LedgerHandle lh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32, PASSWORD);
                try {
                    long lac = lh.getLastAddConfirmed();
                    // Read in bounded batches. One page-index ledger can hold one footer per data page
                    // (hundreds of thousands of entries for a big data ledger); a single readEntries(0,
                    // lac) issues all those reads at once and overwhelms a bookie ("Too many read
                    // requests in progress, disabling autoread" -> operation timeout), which then skips
                    // the ledger and leaves its footers unindexed. Batching caps in-flight reads.
                    for (long start = 0; start <= lac; start += REPLAY_READ_BATCH) {
                        long end = Math.min(start + REPLAY_READ_BATCH - 1, lac);
                        Enumeration<LedgerEntry> en = lh.readEntries(start, end);
                        while (en.hasMoreElements()) {
                            LedgerEntry le = en.nextElement();
                            parseRef(ledgerId, le.getEntryId(), le.getEntry());
                        }
                    }
                } finally {
                    lh.close();
                }
            } catch (BKException.BKNoSuchLedgerExistsException
                    | BKException.BKNoSuchLedgerExistsOnMetadataServerException e) {
                // a chain entry was already GC'd; skip it
            } catch (Exception e) {
                log.warn("StreamLake page index replay skipped ledger {} for {}: {}",
                        ledgerId, metaStore.name(), e.toString());
            }
        }
    }

    private void parseRef(long piLedgerId, long piEntryId, byte[] data) {
        if (data.length < HEADER || data[0] != ENTRY_FOOTER) {
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.get(); // type
        long dataLedgerId = bb.getLong();
        long dataEntryId = bb.getLong();
        refsByDataLedger.computeIfAbsent(dataLedgerId, k -> new ArrayList<>())
                .add(new Ref(piLedgerId, piEntryId, dataEntryId));
    }

    // ---------------------------------------------------------------- write ledger chain

    private void closeHeadQuietly() {
        if (head != null) {
            try {
                head.close();
            } catch (Exception ignore) {
                // openLedger recovery handles an unclosed ledger on the next load
            }
        }
    }

    private byte[] readEntry(long ledgerId, long entryId) throws Exception {
        LedgerHandle lh = handleFor(ledgerId);
        Enumeration<LedgerEntry> en = lh.readEntries(entryId, entryId);
        return en.nextElement().getEntry();
    }

    // ---------------------------------------------------------------- entry codec

    private static byte[] encode(long dataLedgerId, long dataEntryId, byte[] footer) {
        ByteBuffer bb = ByteBuffer.allocate(HEADER + footer.length);
        bb.put(ENTRY_FOOTER);
        bb.putLong(dataLedgerId);
        bb.putLong(dataEntryId);
        bb.put(footer);
        return bb.array();
    }

    private static byte[] footerBody(byte[] entry) {
        byte[] body = new byte[entry.length - HEADER];
        System.arraycopy(entry, HEADER, body, 0, body.length);
        return body;
    }
}
