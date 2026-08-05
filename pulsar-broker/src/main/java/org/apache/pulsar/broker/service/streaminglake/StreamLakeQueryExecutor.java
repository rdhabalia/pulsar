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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.apache.pulsar.client.streaminglake.StreamLakeArrowBatchDecoder;
import org.apache.pulsar.client.streaminglake.StreamLakeHashJoin;
import org.apache.pulsar.client.streaminglake.StreamLakeJoinTable;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeTopK;

/**
 * The StreamLake query execution engine (redesign phase F): composes the read-side primitives into a
 * runnable scan/join/top-K over a topic. {@link StreamLakePruner} yields the surviving pages, a
 * {@link PageReader} returns each page's Arrow batch, the decoder materializes rows, and the
 * {@link StreamLakeScanPredicate} row-filters them exactly (pruning is conservative). Scans compose
 * with {@link StreamLakeHashJoin} and {@link StreamLakeTopK}.
 *
 * <p>This is the executor a SQL frontend (e.g. an Apache Calcite adapter) targets: Calcite parses SQL
 * and pushes filters/projection down into a {@code StreamLakeScanPredicate} + this executor's calls.
 * The {@link PageReader} is the single broker/storage seam -- in a broker it reads the managed-ledger
 * entry and strips the payload framing to the Arrow bytes.
 */
public class StreamLakeQueryExecutor {

    /** Reads the raw Arrow batch bytes for a pruned page (one data-ledger entry). */
    public interface PageReader {
        byte[] readArrowBatch(long ledgerId, long entryId) throws Exception;
    }

    /**
     * Sink for streamed result rows. A scan/join pushes each result row here as it is produced, so the
     * whole result never has to be materialized in memory (the REST layer writes each row straight to
     * the response socket).
     */
    public interface RowConsumer {
        void accept(Object[] row) throws Exception;
    }

    private final StreamLakePruner pruner;
    private final PageReader pageReader;
    private final Executor readExecutor;
    private final int readConcurrency;
    private final StreamLakeQueryMetrics metrics;

    /** Serial reader (no page read-ahead). */
    public StreamLakeQueryExecutor(StreamLakePruner pruner, PageReader pageReader) {
        this(pruner, pageReader, null, 1);
    }

    /**
     * @param readExecutor  pool page reads are dispatched to for parallel prefetch (null = serial)
     * @param readConcurrency max in-flight page reads (&le;1 = serial). Decoding stays single-threaded
     *                        and in page order, so results are identical to a serial scan; only the
     *                        (BookKeeper) reads overlap.
     */
    public StreamLakeQueryExecutor(StreamLakePruner pruner, PageReader pageReader,
            Executor readExecutor, int readConcurrency) {
        this(pruner, pageReader, readExecutor, readConcurrency, new StreamLakeQueryMetrics());
    }

    /**
     * Metrics-bound executor: as it prunes, reads and decodes pages it accumulates the per-query
     * counters into {@code metrics} (rows read, bytes read, pages scanned/kept). One metrics instance is
     * shared across both sides of a join so the numbers are whole-query totals.
     */
    public StreamLakeQueryExecutor(StreamLakePruner pruner, PageReader pageReader,
            Executor readExecutor, int readConcurrency, StreamLakeQueryMetrics metrics) {
        this.pruner = pruner;
        this.pageReader = pageReader;
        this.readExecutor = readExecutor;
        this.readConcurrency = readConcurrency;
        this.metrics = metrics;
        metrics.setReadConcurrency(readConcurrency);
    }

    /** The per-query execution counters this executor accumulates into. */
    public StreamLakeQueryMetrics metrics() {
        return metrics;
    }

    /** Scan a topic: prune to candidate pages, read them, and exactly row-filter by the predicate. */
    public List<Object[]> scan(long fromMs, long toMs, StreamLakeScanPredicate predicate) throws Exception {
        List<Object[]> rows = new ArrayList<>();
        scan(fromMs, toMs, predicate, row -> rows.add(row));
        return rows;
    }

    /**
     * Streaming scan: prune to candidate pages, read them (parallel prefetch), and push each row that
     * passes the predicate to {@code out} — without materializing the whole result.
     */
    public void scan(long fromMs, long toMs, StreamLakeScanPredicate predicate, RowConsumer out)
            throws Exception {
        try (StreamLakeArrowBatchDecoder decoder = new StreamLakeArrowBatchDecoder()) {
            forEachPage(fromMs, toMs, predicate, (p, arrow) -> {
                for (Object[] row : decoder.decodeRows(arrow)) {
                    metrics.incRowsRead();
                    if (predicate.matchesRow(row)) {
                        out.accept(row);
                    }
                }
            });
        }
    }

    /** Scan and keep only the top-K rows by a sort column (ORDER BY ... LIMIT). */
    public List<Object[]> scanTopK(long fromMs, long toMs, StreamLakeScanPredicate predicate,
            int k, int sortColumn, boolean descending) throws Exception {
        StreamLakeTopK topK = new StreamLakeTopK(k, sortColumn, descending);
        topK.offerAll(scan(fromMs, toMs, predicate));
        return topK.results();
    }

    /**
     * Execute a SQL query end to end: parse it with Apache Calcite (via {@link StreamLakeSqlPlanner}),
     * push the WHERE/time predicates into the prune+scan, apply ORDER BY/LIMIT (bounded top-K) and the
     * SELECT projection. {@code timeColumn} (nullable) names the event-time column whose predicates
     * drive the [fromMs, toMs] date-prune window rather than the row filter.
     */
    public List<Object[]> executeSql(String sql, StreamLakeSchema schema, String timeColumn)
            throws Exception {
        StreamLakeSqlPlanner.Plan plan = StreamLakeSqlPlanner.plan(sql, schema, timeColumn);
        List<Object[]> rows;
        if (plan.sortColumn() >= 0 && plan.limit() > 0) {
            rows = scanTopK(plan.fromMs(), plan.toMs(), plan.predicate(), plan.limit(),
                    plan.sortColumn(), plan.descending());
        } else {
            rows = new ArrayList<>(scan(plan.fromMs(), plan.toMs(), plan.predicate()));
            if (plan.sortColumn() >= 0) {
                final int sc = plan.sortColumn();
                Comparator<Object[]> order = Comparator.comparing(
                        r -> asComparable(r[sc]), Comparator.nullsFirst(Comparator.naturalOrder()));
                rows.sort(plan.descending() ? order.reversed() : order);
            }
            if (plan.limit() > 0 && rows.size() > plan.limit()) {
                rows = new ArrayList<>(rows.subList(0, plan.limit()));
            }
        }
        return plan.project(rows);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparable<Object> asComparable(Object v) {
        return (Comparable) v;
    }

    /**
     * Inner-join two scanned sides on a join column with <b>late materialization</b>. The build side
     * (smaller, already pruned) is filtered and its full rows are put into {@code buildTable}; the
     * probe side is streamed: for each surviving probe row only its key column is read to probe the
     * table, and the full probe row is materialized <i>only on a match</i>. Emits
     * {@code concat(probeRow, buildRow)}. {@code buildTable} selects the backend (on-heap vs spilling).
     */
    public List<Object[]> scanInnerJoin(
            long fromMs, long toMs,
            StreamLakeScanPredicate buildPredicate, int buildKeyColumn,
            StreamLakeQueryExecutor probeSide, StreamLakeScanPredicate probePredicate, int probeKeyColumn,
            StreamLakeJoinTable buildTable) throws Exception {
        List<Object[]> out = new ArrayList<>();
        scanInnerJoin(fromMs, toMs, buildPredicate, buildKeyColumn, probeSide, probePredicate,
                probeKeyColumn, buildTable, row -> out.add(row));
        return out;
    }

    /**
     * Streaming inner join: identical to {@link #scanInnerJoin(long, long, StreamLakeScanPredicate, int,
     * StreamLakeQueryExecutor, StreamLakeScanPredicate, int, StreamLakeJoinTable)} but pushes each joined
     * row to {@code out} as it is produced, so the join result is never fully materialized. Only the
     * build side is resident (in {@code buildTable}); the probe side and the output both stream.
     */
    public void scanInnerJoin(
            long fromMs, long toMs,
            StreamLakeScanPredicate buildPredicate, int buildKeyColumn,
            StreamLakeQueryExecutor probeSide, StreamLakeScanPredicate probePredicate, int probeKeyColumn,
            StreamLakeJoinTable buildTable, RowConsumer out) throws Exception {
        try (StreamLakeHashJoin join = new StreamLakeHashJoin(buildKeyColumn, buildTable)) {
            // Phase 1: build side -- filter on predicate columns, materialize the full row only for
            // survivors (into the pluggable build table: on-heap, spilling, or disk-backed).
            scanRows(buildPredicate, fromMs, toMs, (batch, row) -> join.addBuildRow(batch.row(row)));

            // Phase 2: probe side -- filter, read only the key to probe, materialize the full probe row
            // only when it matches a build key; emit each match straight to the sink (no result buffer).
            probeSide.scanRows(probePredicate, fromMs, toMs, (batch, row) -> {
                List<Object[]> buildMatches = join.matches(batch.value(row, probeKeyColumn));
                if (buildMatches.isEmpty()) {
                    return;
                }
                Object[] probeRow = batch.row(row);
                for (Object[] buildRow : buildMatches) {
                    out.accept(StreamLakeHashJoin.concat(probeRow, buildRow));
                }
            });
        }
    }

    /** Convenience overload using an on-heap build table bounded by {@code maxBuildRows}. */
    public List<Object[]> scanInnerJoin(
            long fromMs, long toMs,
            StreamLakeScanPredicate buildPredicate, int buildKeyColumn,
            StreamLakeQueryExecutor probeSide, StreamLakeScanPredicate probePredicate, int probeKeyColumn,
            long maxBuildRows) throws Exception {
        return scanInnerJoin(fromMs, toMs, buildPredicate, buildKeyColumn, probeSide, probePredicate,
                probeKeyColumn, new org.apache.pulsar.client.streaminglake.OnHeapJoinTable(maxBuildRows));
    }

    /** A visitor over the rows of a loaded page batch (used for late-materialized scans). */
    private interface RowVisitor {
        void visit(StreamLakeArrowBatchDecoder.Batch batch, int row) throws Exception;
    }

    /**
     * Prune to candidate pages, then for each page open the Arrow batch and, for every row that passes
     * the predicate (evaluated by reading only the predicate columns), invoke {@code visitor}. The
     * visitor materializes the full row lazily via {@link StreamLakeArrowBatchDecoder.Batch#row}.
     */
    private void scanRows(StreamLakeScanPredicate predicate, long fromMs, long toMs, RowVisitor visitor)
            throws Exception {
        int[] predicateColumns = predicateColumns(predicate);
        try (StreamLakeArrowBatchDecoder decoder = new StreamLakeArrowBatchDecoder()) {
            forEachPage(fromMs, toMs, predicate, (p, arrow) -> {
                try (StreamLakeArrowBatchDecoder.Batch batch = decoder.open(arrow)) {
                    int cols = batch.columnCount();
                    for (int r = 0; r < batch.rowCount(); r++) {
                        metrics.incRowsRead();
                        if (matchesRowLazy(predicate, predicateColumns, batch, r, cols)) {
                            visitor.visit(batch, r);
                        }
                    }
                }
            });
        }
    }

    /** Consumes a page's (pointer, Arrow bytes) in page order; runs on the calling thread. */
    private interface PageConsumer {
        void accept(StreamLakePruner.PagePointer page, byte[] arrow) throws Exception;
    }

    /**
     * Stream the pruned pages (never buffering the full page list) and hand each -- in ledger-then-entry
     * order -- to {@code consumer}. With a read executor and {@code readConcurrency > 1} this keeps up to
     * {@code readConcurrency} page reads in flight (a bounded sliding window) while the consumer
     * decodes/filters the head page on the calling thread, so the (BookKeeper) reads overlap but decoding
     * stays single-threaded and ordered. Otherwise it reads serially. Peak memory is O(readConcurrency)
     * pages regardless of how many pages survive pruning, because the prune pushes pages here one at a
     * time (its {@code accept} blocks once the window is full, back-pressuring the prune traversal).
     */
    private void forEachPage(long fromMs, long toMs, StreamLakeScanPredicate predicate, PageConsumer consumer)
            throws Exception {
        StreamLakePruner.Stats stats = new StreamLakePruner.Stats();
        if (readExecutor == null || readConcurrency <= 1) {
            pruner.prune(fromMs, toMs, predicate, stats, p -> {
                byte[] arrow = pageReader.readArrowBatch(p.ledgerId, p.entryId);
                deliver(consumer, p, arrow);
            });
            metrics.addPruneStats(stats);
            return;
        }
        int window = readConcurrency;
        ArrayDeque<CompletableFuture<byte[]>> inFlight = new ArrayDeque<>(window);
        ArrayDeque<StreamLakePruner.PagePointer> pending = new ArrayDeque<>(window);
        try {
            pruner.prune(fromMs, toMs, predicate, stats, p -> {
                inFlight.add(submitRead(p));
                pending.add(p);
                if (inFlight.size() >= window) {
                    deliver(consumer, pending.poll(), await(inFlight.poll()));
                }
            });
            while (!inFlight.isEmpty()) {
                deliver(consumer, pending.poll(), await(inFlight.poll()));
            }
            metrics.addPruneStats(stats);
        } catch (Exception e) {
            // Cancel any still-in-flight reads so a failure mid-scan doesn't leak the read pool/buffers.
            for (CompletableFuture<byte[]> f : inFlight) {
                f.cancel(true);
            }
            throw e;
        }
    }

    /** Record a read page's size, then hand it (in order) to the consumer. */
    private void deliver(PageConsumer consumer, StreamLakePruner.PagePointer p, byte[] arrow)
            throws Exception {
        metrics.recordPageRead(arrow.length);
        consumer.accept(p, arrow);
    }

    /** Await a read, unwrapping the (possibly double-wrapped) cause as the original checked exception. */
    private static byte[] await(CompletableFuture<byte[]> f) throws Exception {
        try {
            return f.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() instanceof CompletionException ? e.getCause().getCause()
                    : e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private CompletableFuture<byte[]> submitRead(StreamLakePruner.PagePointer p) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return pageReader.readArrowBatch(p.ledgerId, p.entryId);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, readExecutor);
    }

    // Evaluate the predicate by materializing only its columns into a sparse full-width row.
    private static boolean matchesRowLazy(StreamLakeScanPredicate predicate, int[] predicateColumns,
            StreamLakeArrowBatchDecoder.Batch batch, int row, int columnCount) {
        Object[] sparse = new Object[columnCount];
        for (int c : predicateColumns) {
            sparse[c] = batch.value(row, c);
        }
        return predicate.matchesRow(sparse);
    }

    private static int[] predicateColumns(StreamLakeScanPredicate predicate) {
        return predicate.columns().stream()
                .mapToInt(StreamLakeScanPredicate.ColumnPredicate::columnIndex)
                .distinct().toArray();
    }
}
