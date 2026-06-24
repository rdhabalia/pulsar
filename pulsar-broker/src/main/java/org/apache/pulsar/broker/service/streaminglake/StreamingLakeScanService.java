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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Server-side Streaming Lake scan over a topic's managed ledger.
 *
 * <p>Two modes:
 * <ul>
 *   <li>{@link #scan} — read-all-and-filter (correct, but reads every entry).</li>
 *   <li>{@link #buildPageIndex} + {@link #prunedScan} — the optimized path: a page-range
 *       index records, per page, the min/max of the date partition and indexed columns;
 *       a scan then reads <b>only the candidate pages</b> whose ranges overlap the query,
 *       skipping the rest. This is the page-pruning performance win, executed at the
 *       broker over real BookKeeper storage.</li>
 * </ul>
 *
 * <p>Unbounded sentinels ({@code Integer/Long.MIN/MAX_VALUE}) mean "no bound", so a full
 * {@code SELECT *} is a scan with every bound unbounded (no page is pruned).
 */
public final class StreamingLakeScanService {

    private StreamingLakeScanService() {
    }

    /** A scanned row: the indexed columns from a message's metadata. */
    public static final class Row {
        public final String name;
        public final int departmentId;
        public final int salary;
        public final long eventTime;

        Row(String name, int departmentId, int salary, long eventTime) {
            this.name = name;
            this.departmentId = departmentId;
            this.salary = salary;
            this.eventTime = eventTime;
        }

        @Override
        public String toString() {
            return name + "," + departmentId + "," + salary + "," + eventTime;
        }
    }

    /** Per-page min/max ranges (the columnar page metadata) + the page's entry positions. */
    public static final class PageMeta {
        final List<Position> positions = new ArrayList<>();
        long etMin = Long.MAX_VALUE;
        long etMax = Long.MIN_VALUE;
        int deptMin = Integer.MAX_VALUE;
        int deptMax = Integer.MIN_VALUE;
        int salaryMin = Integer.MAX_VALUE;
        int salaryMax = Integer.MIN_VALUE;

        void accumulate(Position pos, Row r) {
            positions.add(pos);
            etMin = Math.min(etMin, r.eventTime);
            etMax = Math.max(etMax, r.eventTime);
            deptMin = Math.min(deptMin, r.departmentId);
            deptMax = Math.max(deptMax, r.departmentId);
            salaryMin = Math.min(salaryMin, r.salary);
            salaryMax = Math.max(salaryMax, r.salary);
        }

        /** Conservative overlap: could this page contain a row satisfying the query? */
        boolean couldMatch(long from, long to, int deptGt, int deptLt, int salaryGt) {
            if (etMax < from || etMin > to) {
                return false; // date partition out of range
            }
            // departmentId > deptGt AND departmentId < deptLt
            if (!(deptMax > deptGt && deptMin < deptLt)) {
                return false;
            }
            // salary > salaryGt
            return salaryMax > salaryGt;
        }
    }

    // -------------------------------------------------------------- full scan

    /** SELECT * WHERE eventTime in [from,to] AND dept>deptGt AND dept<deptLt AND salary>salaryGt. */
    public static List<Row> scan(ManagedLedger ledger, long from, long to,
                                 int deptGt, int deptLt, int salaryGt) throws Exception {
        List<Row> out = new ArrayList<>();
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST);
        try {
            while (cursor.hasMoreEntries()) {
                List<Entry> entries = cursor.readEntries(100);
                for (Entry entry : entries) {
                    try {
                        Row r = toRow(entry);
                        if (r != null && matches(r, from, to, deptGt, deptLt, salaryGt)) {
                            out.add(r);
                        }
                    } finally {
                        entry.release();
                    }
                }
            }
        } finally {
            cursor.close();
        }
        return out;
    }

    // -------------------------------------------------- page index + pruned scan

    /**
     * Build the page-range index by grouping consecutive entries into pages of
     * {@code pageSize} and recording each page's min/max ranges + positions. In
     * production this is maintained on the write path; here it is built once and reused.
     */
    public static List<PageMeta> buildPageIndex(ManagedLedger ledger, int pageSize) throws Exception {
        List<PageMeta> pages = new ArrayList<>();
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST);
        try {
            PageMeta current = new PageMeta();
            while (cursor.hasMoreEntries()) {
                List<Entry> entries = cursor.readEntries(100);
                for (Entry entry : entries) {
                    try {
                        Row r = toRow(entry);
                        if (r == null) {
                            continue;
                        }
                        current.accumulate(entry.getPosition(), r);
                        if (current.positions.size() >= pageSize) {
                            pages.add(current);
                            current = new PageMeta();
                        }
                    } finally {
                        entry.release();
                    }
                }
            }
            if (!current.positions.isEmpty()) {
                pages.add(current);
            }
        } finally {
            cursor.close();
        }
        return pages;
    }

    /**
     * Pruned scan: read only the candidate pages whose ranges overlap the query.
     * {@code entriesReadOut[0]} is set to the number of entries actually read from
     * storage (so callers/tests can prove pruning happened).
     */
    public static List<Row> prunedScan(ManagedLedger ledger, List<PageMeta> index,
                                       long from, long to, int deptGt, int deptLt, int salaryGt,
                                       int[] entriesReadOut) throws Exception {
        List<Row> out = new ArrayList<>();
        int entriesRead = 0;
        for (PageMeta page : index) {
            if (!page.couldMatch(from, to, deptGt, deptLt, salaryGt)) {
                continue; // prune: do not read this page's entries at all
            }
            for (Position pos : page.positions) {
                Entry entry = readEntry(ledger, pos);
                entriesRead++;
                try {
                    Row r = toRow(entry);
                    if (r != null && matches(r, from, to, deptGt, deptLt, salaryGt)) {
                        out.add(r);
                    }
                } finally {
                    entry.release();
                }
            }
        }
        if (entriesReadOut != null && entriesReadOut.length > 0) {
            entriesReadOut[0] = entriesRead;
        }
        return out;
    }

    // ------------------------------------------------------------------ helpers

    private static boolean matches(Row r, long from, long to, int deptGt, int deptLt, int salaryGt) {
        return r.eventTime >= from && r.eventTime <= to
                && r.departmentId > deptGt && r.departmentId < deptLt && r.salary > salaryGt;
    }

    /** Extract the indexed columns from a message's metadata; null for marker/unknown entries. */
    private static Row toRow(Entry entry) {
        MessageMetadata md = entry.getMessageMetadata();
        if (md == null) {
            ByteBuf buf = entry.getDataBuffer();
            md = Commands.peekMessageMetadata(buf, "streaming-lake-scan", -1);
        }
        if (md == null || md.hasMarkerType()) {
            return null;
        }
        String name = null;
        int dept = Integer.MIN_VALUE;
        int salary = Integer.MIN_VALUE;
        for (KeyValue kv : md.getPropertiesList()) {
            String k = kv.getKey();
            if ("name".equals(k)) {
                name = kv.getValue();
            } else if ("departmentId".equals(k)) {
                dept = Integer.parseInt(kv.getValue());
            } else if ("salary".equals(k)) {
                salary = Integer.parseInt(kv.getValue());
            }
        }
        return new Row(name, dept, salary, md.getEventTime());
    }

    private static Entry readEntry(ManagedLedger ledger, Position pos) throws Exception {
        CompletableFuture<Entry> future = new CompletableFuture<>();
        ledger.asyncReadEntry(pos, new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                future.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future.get(30, TimeUnit.SECONDS);
    }
}
