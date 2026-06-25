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
import java.util.Enumeration;
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
 * Durable date-partition index for a StreamLake topic (design point 7).
 *
 * <p>Instead of keeping the per-ledger date ranges only in memory (lost on reload), they live in
 * a single append-only "date-partition-list" BookKeeper ledger: each entry is
 * {@code {dataLedgerId, minDate, maxDate}} (e.g. {@code 2026-01-01 -> L1}, {@code 2026-01-02 ->
 * L2}). The list ledger's id is stored in the managed-ledger properties, so it's the only extra
 * pointer in metadata. At one ledger per day, even a couple of years stays well under 1K entries.
 *
 * <p>On load the broker replays the previous list ledger to rebuild the in-memory view, writes the
 * merged list into a fresh ledger (handling the case where the old one was fenced/closed), repoints
 * the property, and deletes the old ledger -- so a fenced list always self-heals into a new one.
 */
public class StreamLakeDateIndex {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeDateIndex.class);
    private static final String PROP = "streamlake.datePartitionLedgerId";
    private static final byte[] PASSWORD = "streamlake-date".getBytes();
    private static final int ENTRY_SIZE = 24; // ledgerId(8) + minDate(8) + maxDate(8)

    private final BookKeeper bk;
    private final ManagedLedger ml;
    private final ConcurrentMap<Long, long[]> ranges = new ConcurrentHashMap<>();
    private volatile LedgerHandle writeLedger; // null => in-memory only (BK unavailable)

    private StreamLakeDateIndex(BookKeeper bk, ManagedLedger ml) {
        this.bk = bk;
        this.ml = ml;
    }

    /** Load (replay + rotate) the date-partition-list ledger; never throws -- falls back to memory. */
    public static StreamLakeDateIndex open(BookKeeper bk, ManagedLedger ml) {
        StreamLakeDateIndex idx = new StreamLakeDateIndex(bk, ml);
        try {
            idx.loadAndRotate();
        } catch (Exception e) {
            log.warn("StreamLake date index falling back to in-memory for {}: {}", ml.getName(),
                    e.toString());
            idx.writeLedger = null;
        }
        return idx;
    }

    private synchronized void loadAndRotate() throws Exception {
        Long oldId = null;
        String prop = ml.getProperties().get(PROP);
        if (prop != null) {
            oldId = Long.parseLong(prop);
            try {
                LedgerHandle old = bk.openLedger(oldId, BookKeeper.DigestType.CRC32, PASSWORD);
                long lac = old.getLastAddConfirmed();
                if (lac >= 0) {
                    Enumeration<LedgerEntry> en = old.readEntries(0, lac);
                    while (en.hasMoreElements()) {
                        mergeEntry(en.nextElement().getEntry());
                    }
                }
                old.close();
            } catch (BKException.BKNoSuchLedgerExistsException | BKException.BKNoSuchLedgerExistsOnMetadataServerException e) {
                oldId = null; // pointer dangled; start fresh
            }
        }

        LedgerHandle fresh = bk.createLedger(1, 1, BookKeeper.DigestType.CRC32, PASSWORD);
        for (Map.Entry<Long, long[]> e : ranges.entrySet()) {
            fresh.addEntry(encode(e.getKey(), e.getValue()[0], e.getValue()[1]));
        }
        this.writeLedger = fresh;
        ml.setProperty(PROP, Long.toString(fresh.getId()));
        if (oldId != null && oldId != fresh.getId()) {
            try {
                bk.deleteLedger(oldId);
            } catch (Exception ignore) {
                // best-effort cleanup
            }
        }
    }

    /** Record (and durably append) a data ledger's date range, merging with any existing range. */
    public synchronized void record(long dataLedgerId, long minDate, long maxDate) {
        long[] cur = ranges.get(dataLedgerId);
        long newMin = cur == null ? minDate : Math.min(cur[0], minDate);
        long newMax = cur == null ? maxDate : Math.max(cur[1], maxDate);
        if (cur != null && cur[0] == newMin && cur[1] == newMax) {
            return; // unchanged -> nothing to persist
        }
        ranges.put(dataLedgerId, new long[]{newMin, newMax});
        if (writeLedger == null) {
            return;
        }
        try {
            writeLedger.addEntry(encode(dataLedgerId, newMin, newMax));
        } catch (Exception e) {
            // list ledger fenced/closed: rotate to a fresh one (replays current map) and retry once.
            try {
                loadAndRotate();
                if (writeLedger != null) {
                    writeLedger.addEntry(encode(dataLedgerId, newMin, newMax));
                }
            } catch (Exception ex) {
                log.warn("StreamLake date index append failed for {}: {}", ml.getName(), ex.toString());
            }
        }
    }

    public Map<Long, long[]> ranges() {
        return ranges;
    }

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

    private void mergeEntry(byte[] b) {
        long ledgerId = getLong(b, 0);
        long min = getLong(b, 8);
        long max = getLong(b, 16);
        ranges.merge(ledgerId, new long[]{min, max},
                (a, c) -> new long[]{Math.min(a[0], c[0]), Math.max(a[1], c[1])});
    }

    private static byte[] encode(long ledgerId, long minDate, long maxDate) {
        ByteBuffer bb = ByteBuffer.allocate(ENTRY_SIZE);
        bb.putLong(ledgerId).putLong(minDate).putLong(maxDate);
        return bb.array();
    }

    private static long getLong(byte[] b, int off) {
        return ((long) (b[off] & 0xFF) << 56) | ((long) (b[off + 1] & 0xFF) << 48)
                | ((long) (b[off + 2] & 0xFF) << 40) | ((long) (b[off + 3] & 0xFF) << 32)
                | ((long) (b[off + 4] & 0xFF) << 24) | ((long) (b[off + 5] & 0xFF) << 16)
                | ((long) (b[off + 6] & 0xFF) << 8) | (b[off + 7] & 0xFF);
    }
}
