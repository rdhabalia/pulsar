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
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.apache.pulsar.common.policies.data.StreamLakeQueryResult;

/**
 * Broker-side query coordinator: turns a SQL string into result rows by planning it with
 * {@link StreamLakeSqlPlanner} and executing it against one or two per-table
 * {@link StreamLakeQueryService}s (single-table scan or two-table inner equi-join). Table names in the
 * SQL are resolved to their topics via the supplied resolver.
 *
 * <p><b>Streaming.</b> {@link #prepare} plans the query (resolving tables/schemas up front so a bad
 * query fails before any output) and returns a {@link Prepared} whose {@link Prepared#stream} pushes
 * each result row to a sink as it is produced — the whole result is never materialized on the broker,
 * so a multi-GB result streams straight to the response socket without OOMing. {@link #executeSql} is a
 * buffered convenience (collects the stream) for small results and tests.
 */
public final class StreamLakeQueryCoordinator {

    private final Function<String, StreamLakeQueryService> serviceByTable;

    public StreamLakeQueryCoordinator(Function<String, StreamLakeQueryService> serviceByTable) {
        this.serviceByTable = serviceByTable;
    }

    /** Receives each result row as it is produced (rows are not retained). */
    public interface RowSink {
        void row(Object[] row) throws Exception;
    }

    private interface StreamRunner {
        void run(RowSink sink) throws Exception;
    }

    /** A planned query: the column header (known up front) plus a streaming executor. */
    public static final class Prepared {
        private final List<String> columns;
        private final StreamRunner runner;
        private final StreamLakeQueryMetrics metrics;

        private Prepared(List<String> columns, StreamRunner runner, StreamLakeQueryMetrics metrics) {
            this.columns = columns;
            this.runner = runner;
            this.metrics = metrics;
        }

        /** Result column names, in output order (available before any row is produced). */
        public List<String> columns() {
            return columns;
        }

        /** Execute the query, pushing each result row to {@code sink} as it is produced. */
        public void stream(RowSink sink) throws Exception {
            runner.run(sink);
        }

        /** Per-query broker-side execution counters, populated as {@link #stream} runs. */
        public StreamLakeQueryMetrics metrics() {
            return metrics;
        }
    }

    /**
     * Plan {@code sql} and resolve its tables (may throw {@link IllegalArgumentException} for a bad
     * query or unknown/!StreamLake table) so the caller can fail cleanly before streaming any bytes.
     * An {@code EXPLAIN <query>} prefix returns a single {@code plan} row describing the chosen operator
     * + estimates instead of executing.
     */
    public Prepared prepare(String sql) {
        String trimmed = sql.trim();
        boolean explain = trimmed.length() >= 8 && trimmed.regionMatches(true, 0, "EXPLAIN ", 0, 8);
        String query = explain ? trimmed.substring(8) : sql;

        Function<String, StreamLakeSchema> schemas = table -> {
            StreamLakeQueryService s = serviceByTable.apply(table);
            return s == null ? null : s.schema();
        };
        StreamLakeQueryMetrics metrics = new StreamLakeQueryMetrics();
        // Expose the pseudo event-time column so `WHERE __event_time BETWEEN ms1 AND ms2` prunes whole
        // data ledgers by the catalog's per-ledger ingest-time bounds (single-table + GROUP BY paths).
        StreamLakeSqlPlanner.Planned planned = StreamLakeSqlPlanner.planStatement(
                query, schemas, t -> StreamLakeSqlPlanner.EVENT_TIME_COLUMN);
        if (planned.isJoin()) {
            return prepareJoin(planned.join(), explain, metrics);
        }
        if (planned.isGroupBy()) {
            return prepareGroupBy(planned.groupBy(), explain, metrics);
        }
        return prepareSingle(query, planned.single(), explain, metrics);
    }

    private Prepared prepareGroupBy(StreamLakeSqlPlanner.GroupByPlan gp, boolean explain,
            StreamLakeQueryMetrics metrics) {
        StreamLakeQueryService svc = require(gp.table());
        if (explain) {
            return planRow("GROUPBY table=" + gp.table() + " groupCols=" + gp.groupCols().length
                    + " aggs=" + gp.aggs().size() + " output=" + gp.columnNames(), metrics);
        }
        StreamRunner runner = sink -> StreamLakeGroupBy.aggregate(svc.executor(metrics), gp,
                svc.joinSpillDir(), svc.rocksdbBlockCacheBytes(), svc.rocksdbWriteBufferBytes(),
                row -> sink.row(row));
        return new Prepared(gp.columnNames(), runner, metrics);
    }

    // Cost-based join planning: build the smaller pruned side; broadcast it if it fits the build-memory
    // budget, else partition both sides (Grace). Guards a runaway (quadratic) result.
    private Prepared prepareJoin(StreamLakeSqlPlanner.JoinPlan jp, boolean explain,
            StreamLakeQueryMetrics metrics) {
        StreamLakeQueryService left = require(jp.leftTable());
        StreamLakeQueryService right = require(jp.rightTable());
        StreamLakeStatistics.Estimate le = estimate(left, jp.leftPredicate());
        StreamLakeStatistics.Estimate re = estimate(right, jp.rightPredicate());

        boolean buildLeft = le.bytes <= re.bytes;
        StreamLakeQueryService buildSvc = buildLeft ? left : right;
        StreamLakeStatistics.Estimate buildEst = buildLeft ? le : re;
        StreamLakeStatistics.Estimate probeEst = buildLeft ? re : le;
        long budget = buildSvc.joinBuildMemoryBudget();
        // Strategy: honor a forced joinStrategy, else choose by cost (build fits budget -> BROADCAST,
        // else GRACE partitioned, or a RocksDB broadcast table when joinLargeBuildUsesRocksDb).
        JoinOp op;
        switch (buildSvc.joinStrategy()) {
            case BROADCAST: op = JoinOp.BROADCAST; break;
            case GRACE: op = JoinOp.GRACE; break;
            case ROCKSDB: op = JoinOp.ROCKSDB; break;
            default:
                op = buildEst.bytes <= budget ? JoinOp.BROADCAST
                        : (buildSvc.joinLargeBuildUsesRocksDb() ? JoinOp.ROCKSDB : JoinOp.GRACE);
        }
        int partitions = op == JoinOp.GRACE ? (int) Math.min(buildSvc.joinMaxPartitions(),
                Math.max(2, (buildEst.bytes + budget - 1) / Math.max(1, budget))) : 1;

        // Runaway guard (coarse until cardinality sketches land): a FK-style join yields ~max(rows).
        long estResultRows = Math.max(le.rows, re.rows);
        long guard = buildSvc.runawayResultRows();
        if (guard > 0 && estResultRows > guard) {
            throw new IllegalArgumentException("Estimated result (~" + estResultRows + " rows) exceeds "
                    + "runawayResultRows=" + guard + "; add a more selective predicate");
        }

        String plan = String.format("JOIN strategy=%s build=%s(%s) probe=%s(%s) partitions=%d "
                        + "budgetBytes=%d estResultRows~%d",
                op, buildLeft ? jp.leftTable() : jp.rightTable(), buildEst,
                buildLeft ? jp.rightTable() : jp.leftTable(), probeEst,
                partitions, budget, estResultRows);
        if (explain) {
            return planRow(plan, metrics);
        }

        StreamLakeQueryExecutor buildExec = buildSvc.executor(metrics);
        StreamLakeQueryExecutor probeExec = (buildLeft ? right : left).executor(metrics);
        StreamLakeScanPredicate buildPred = buildLeft ? jp.leftPredicate() : jp.rightPredicate();
        int buildKey = buildLeft ? jp.leftKey() : jp.rightKey();
        StreamLakeScanPredicate probePred = buildLeft ? jp.rightPredicate() : jp.leftPredicate();
        int probeKey = buildLeft ? jp.rightKey() : jp.leftKey();
        // executor + grace both emit concat(probeRow, buildRow); map to natural [left..., right...].
        Function<Object[], Object[]> combine = buildLeft ? jp::combineFromBuildLeft : jp::combineFromBuildRight;

        StreamRunner runner;
        switch (op) {
            case GRACE: {
                final int nParts = partitions;
                runner = sink -> StreamLakeGraceJoin.join(buildExec, 0, Long.MAX_VALUE, buildPred, buildKey,
                        probeExec, probePred, probeKey, nParts, buildSvc.joinSpillDir(),
                        buildSvc.joinMaxBuildRows(), row -> sink.row(combine.apply(row)));
                break;
            }
            case ROCKSDB:
                // Broadcast join whose build table is RocksDB (keys+values on disk).
                runner = sink -> buildExec.scanInnerJoin(0, Long.MAX_VALUE, buildPred, buildKey,
                        probeExec, probePred, probeKey,
                        buildSvc.newBuildTable(StreamLakeQueryService.Backend.ROCKSDB),
                        row -> sink.row(combine.apply(row)));
                break;
            default: // BROADCAST (on-heap or spilling table per config)
                runner = sink -> buildExec.scanInnerJoin(0, Long.MAX_VALUE, buildPred, buildKey,
                        probeExec, probePred, probeKey, buildSvc.newBuildTable(),
                        row -> sink.row(combine.apply(row)));
        }
        return new Prepared(jp.columnNames(), runner, metrics);
    }

    private enum JoinOp { BROADCAST, GRACE, ROCKSDB }

    private Prepared prepareSingle(String query, StreamLakeSqlPlanner.Plan p, boolean explain,
            StreamLakeQueryMetrics metrics) {
        StreamLakeQueryService svc = require(p.table());
        List<String> columns = singleColumnNames(p, svc.schema());
        if (explain) {
            String order = p.sortColumn() >= 0 ? " orderBy=col" + p.sortColumn()
                    + (p.descending() ? " DESC" : " ASC") + (p.limit() > 0 ? " limit=" + p.limit() : "") : "";
            return planRow("SCAN " + p.table() + "(" + estimate(svc, p.predicate()) + ")" + order, metrics);
        }
        StreamLakeQueryExecutor exec = svc.executor(metrics);
        StreamRunner runner;
        if (p.sortColumn() >= 0 && p.limit() <= 0) {
            // Unbounded ORDER BY -> RocksDB external sort (streamed, bounded memory).
            StreamLakeType sortType = svc.schema().columns().get(p.sortColumn()).type();
            runner = sink -> StreamLakeExternalSort.sort(exec, p.fromMs(), p.toMs(),
                    p.predicate(), p.sortColumn(), sortType, p.descending(), p::projectRow,
                    svc.joinSpillDir(), svc.rocksdbBlockCacheBytes(), svc.rocksdbWriteBufferBytes(),
                    row -> sink.row(row));
        } else if (p.sortColumn() >= 0) {
            // ORDER BY ... LIMIT k -> bounded top-K (cheaper than an external sort).
            runner = sink -> {
                for (Object[] r : exec.executeSql(query, svc.schema(), null)) {
                    sink.row(r);
                }
            };
        } else {
            runner = sink -> exec.scan(p.fromMs(), p.toMs(), p.predicate(),
                    row -> sink.row(p.projectRow(row)));
        }
        return new Prepared(columns, runner, metrics);
    }

    private static Prepared planRow(String text, StreamLakeQueryMetrics metrics) {
        return new Prepared(List.of("plan"), sink -> sink.row(new Object[]{text}), metrics);
    }

    /** Buffered convenience: collect the streamed rows into a {@link StreamLakeQueryResult}. */
    public StreamLakeQueryResult executeSql(String sql) throws Exception {
        long t0 = System.nanoTime();
        Prepared prepared = prepare(sql);
        List<List<Object>> rows = new ArrayList<>();
        prepared.stream(row -> rows.add(new ArrayList<>(Arrays.asList(row))));
        long latencyMs = (System.nanoTime() - t0) / 1_000_000;
        return new StreamLakeQueryResult(prepared.columns(), rows, latencyMs);
    }

    private StreamLakeQueryService require(String table) {
        StreamLakeQueryService svc = serviceByTable.apply(table);
        if (svc == null) {
            throw new IllegalArgumentException("Table not found or not a loaded StreamLake topic: " + table);
        }
        return svc;
    }

    // Metadata-only pruned-size estimate for a join side; a failed estimate is treated as "large" so
    // the side is not chosen as the (resident) build side.
    private static StreamLakeStatistics.Estimate estimate(StreamLakeQueryService svc,
            StreamLakeScanPredicate predicate) {
        try {
            return svc.estimate(0, Long.MAX_VALUE, predicate);
        } catch (Exception e) {
            long big = Long.MAX_VALUE / 4;
            return new StreamLakeStatistics.Estimate(big, 0, big, big);
        }
    }

    private static List<String> singleColumnNames(StreamLakeSqlPlanner.Plan plan, StreamLakeSchema schema) {
        List<String> all = new ArrayList<>();
        for (StreamLakeSchema.Column c : schema.columns()) {
            all.add(c.name());
        }
        int[] proj = plan.projection();
        if (proj == null) {
            return all;
        }
        List<String> out = new ArrayList<>(proj.length);
        for (int idx : proj) {
            out.add(all.get(idx));
        }
        return out;
    }
}
