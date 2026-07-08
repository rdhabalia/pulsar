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
    private final ManagedLedger ml;
    private final StreamLakeMetaStore metaStore;
    private final long maxHeadBytes;

    private final Map<Long, List<Ref>> refsByDataLedger = new HashMap<>();
    private final List<Long> chain = new ArrayList<>();
    private final Map<Long, LedgerHandle> readHandles = new HashMap<>();
    private LedgerHandle head;
    private long headBytes;

    private StreamLakePageIndex(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                long maxHeadBytes) {
        this.bk = bk;
        this.ml = ml;
        this.metaStore = metaStore;
        this.maxHeadBytes = maxHeadBytes > 0 ? maxHeadBytes : DEFAULT_MAX_HEAD_BYTES;
    }

    /** Load and replay the page-index ledger chain; never throws (falls back to an empty index). */
    public static StreamLakePageIndex open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore) {
        return open(bk, ml, metaStore, DEFAULT_MAX_HEAD_BYTES);
    }

    public static StreamLakePageIndex open(BookKeeper bk, ManagedLedger ml, StreamLakeMetaStore metaStore,
                                           long maxHeadBytes) {
        StreamLakePageIndex idx = new StreamLakePageIndex(bk, ml, metaStore, maxHeadBytes);
        try {
            idx.chain.addAll(metaStore.read().pageIndexLedgerIds);
            idx.replay();
        } catch (Exception e) {
            log.warn("StreamLake page index falling back to empty for {}: {}", ml.getName(), e.toString());
        }
        return idx;
    }

    /**
     * Durably append a batch's stats footer, keyed by the data-ledger entry it describes. The footer
     * is sliced by the caller from the message payload (never parsed). Rolls the head ledger first
     * when it would exceed {@code maxHeadBytes}.
     */
    public synchronized void appendFooter(long dataLedgerId, long dataEntryId, byte[] footer)
            throws Exception {
        byte[] entry = encode(dataLedgerId, dataEntryId, footer);
        ensureHeadFor(entry.length);
        long piEntryId = addToHead(entry);
        refsByDataLedger.computeIfAbsent(dataLedgerId, k -> new ArrayList<>())
                .add(new Ref(head.getId(), piEntryId, dataEntryId));
        headBytes += entry.length;
    }

    /** The stored footers for a data ledger, ordered by data-entry, read on demand from the chain. */
    public synchronized List<PageFooter> footersFor(long dataLedgerId) throws Exception {
        List<Ref> refs = refsByDataLedger.get(dataLedgerId);
        if (refs == null || refs.isEmpty()) {
            return Collections.emptyList();
        }
        List<PageFooter> out = new ArrayList<>(refs.size());
        for (Ref ref : refs) {
            byte[] data = readEntry(ref.piLedgerId, ref.piEntryId);
            out.add(new PageFooter(ref.dataEntryId, footerBody(data)));
        }
        return out;
    }

    /** Whether any footer has been recorded for a data ledger. */
    public synchronized boolean covers(long dataLedgerId) {
        List<Ref> refs = refsByDataLedger.get(dataLedgerId);
        return refs != null && !refs.isEmpty();
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
                long lac = lh.getLastAddConfirmed();
                if (lac >= 0) {
                    Enumeration<LedgerEntry> en = lh.readEntries(0, lac);
                    while (en.hasMoreElements()) {
                        LedgerEntry le = en.nextElement();
                        parseRef(ledgerId, le.getEntryId(), le.getEntry());
                    }
                }
                lh.close();
            } catch (BKException.BKNoSuchLedgerExistsException
                    | BKException.BKNoSuchLedgerExistsOnMetadataServerException e) {
                // a chain entry was already GC'd; skip it
            } catch (Exception e) {
                log.warn("StreamLake page index replay skipped ledger {} for {}: {}",
                        ledgerId, ml.getName(), e.toString());
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

    private void ensureHeadFor(int entryLen) throws Exception {
        if (head == null) {
            rotateHead();
        } else if (headBytes > 0 && headBytes + entryLen > maxHeadBytes) {
            rotateHead();
        }
    }

    private void rotateHead() throws Exception {
        closeHeadQuietly();
        LedgerHandle fresh = bk.createLedger(1, 1, BookKeeper.DigestType.CRC32, PASSWORD);
        chain.add(fresh.getId());
        metaStore.setPageIndexLedgerIds(chain);
        head = fresh;
        headBytes = 0;
    }

    private long addToHead(byte[] entry) throws Exception {
        try {
            return head.addEntry(entry);
        } catch (Exception e) {
            // head fenced/closed -> start a new head and retry once
            head = null;
            rotateHead();
            return head.addEntry(entry);
        }
    }

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
        if (head != null && head.getId() == ledgerId) {
            Enumeration<LedgerEntry> en = head.readEntries(entryId, entryId);
            return en.nextElement().getEntry();
        }
        LedgerHandle lh = readHandles.get(ledgerId);
        if (lh == null) {
            lh = bk.openLedger(ledgerId, BookKeeper.DigestType.CRC32, PASSWORD);
            readHandles.put(ledgerId, lh);
        }
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
