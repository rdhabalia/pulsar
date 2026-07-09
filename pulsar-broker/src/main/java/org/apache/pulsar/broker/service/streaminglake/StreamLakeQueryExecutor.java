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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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

    private final StreamLakePruner pruner;
    private final PageReader pageReader;

    public StreamLakeQueryExecutor(StreamLakePruner pruner, PageReader pageReader) {
        this.pruner = pruner;
        this.pageReader = pageReader;
    }

    /** Scan a topic: prune to candidate pages, read them, and exactly row-filter by the predicate. */
    public List<Object[]> scan(long fromMs, long toMs, StreamLakeScanPredicate predicate) throws Exception {
        List<StreamLakePruner.PagePointer> pages = pruner.prune(fromMs, toMs, predicate);
        List<Object[]> rows = new ArrayList<>();
        try (StreamLakeArrowBatchDecoder decoder = new StreamLakeArrowBatchDecoder()) {
            for (StreamLakePruner.PagePointer p : pages) {
                byte[] arrow = pageReader.readArrowBatch(p.ledgerId, p.entryId);
                for (Object[] row : decoder.decodeRows(arrow)) {
                    if (predicate.matchesRow(row)) {
                        rows.add(row);
                    }
                }
            }
        }
        return rows;
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
        try (StreamLakeHashJoin join = new StreamLakeHashJoin(buildKeyColumn, buildTable)) {
            // Phase 1: build side -- filter on predicate columns, materialize the full row only for
            // survivors (the build side is small and any of its columns may be projected downstream).
            scanRows(buildPredicate, fromMs, toMs, (batch, row) -> join.addBuildRow(batch.row(row)));

            // Phase 2: probe side -- filter, read only the key to probe, materialize the full probe row
            // only when it matches a build key (so non-matching probe rows never allocate wide columns).
            probeSide.scanRows(probePredicate, fromMs, toMs, (batch, row) -> {
                List<Object[]> buildMatches = join.matches(batch.value(row, probeKeyColumn));
                if (buildMatches.isEmpty()) {
                    return;
                }
                Object[] probeRow = batch.row(row);
                for (Object[] buildRow : buildMatches) {
                    out.add(StreamLakeHashJoin.concat(probeRow, buildRow));
                }
            });
        }
        return out;
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
        List<StreamLakePruner.PagePointer> pages = pruner.prune(fromMs, toMs, predicate);
        try (StreamLakeArrowBatchDecoder decoder = new StreamLakeArrowBatchDecoder()) {
            for (StreamLakePruner.PagePointer p : pages) {
                byte[] arrow = pageReader.readArrowBatch(p.ledgerId, p.entryId);
                try (StreamLakeArrowBatchDecoder.Batch batch = decoder.open(arrow)) {
                    int cols = batch.columnCount();
                    for (int r = 0; r < batch.rowCount(); r++) {
                        if (matchesRowLazy(predicate, predicateColumns, batch, r, cols)) {
                            visitor.visit(batch, r);
                        }
                    }
                }
            }
        }
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
