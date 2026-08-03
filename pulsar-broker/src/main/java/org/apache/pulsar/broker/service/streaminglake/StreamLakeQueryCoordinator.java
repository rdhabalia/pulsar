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
import org.apache.pulsar.client.streaminglake.OnHeapJoinTable;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
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

        private Prepared(List<String> columns, StreamRunner runner) {
            this.columns = columns;
            this.runner = runner;
        }

        /** Result column names, in output order (available before any row is produced). */
        public List<String> columns() {
            return columns;
        }

        /** Execute the query, pushing each result row to {@code sink} as it is produced. */
        public void stream(RowSink sink) throws Exception {
            runner.run(sink);
        }
    }

    /**
     * Plan {@code sql} and resolve its tables (may throw {@link IllegalArgumentException} for a bad
     * query or unknown/!StreamLake table) so the caller can fail cleanly before streaming any bytes.
     */
    public Prepared prepare(String sql) {
        Function<String, StreamLakeSchema> schemas = table -> {
            StreamLakeQueryService s = serviceByTable.apply(table);
            return s == null ? null : s.schema();
        };
        StreamLakeSqlPlanner.Planned planned = StreamLakeSqlPlanner.planStatement(sql, schemas, t -> null);

        if (planned.isJoin()) {
            StreamLakeSqlPlanner.JoinPlan jp = planned.join();
            StreamLakeQueryService left = require(jp.leftTable());
            StreamLakeQueryService right = require(jp.rightTable());
            StreamRunner runner = sink -> left.executor().scanInnerJoin(0, Long.MAX_VALUE,
                    jp.leftPredicate(), jp.leftKey(), right.executor(), jp.rightPredicate(), jp.rightKey(),
                    new OnHeapJoinTable(Long.MAX_VALUE), row -> sink.row(jp.combineRow(row)));
            return new Prepared(jp.columnNames(), runner);
        }

        StreamLakeSqlPlanner.Plan p = planned.single();
        StreamLakeQueryService svc = require(p.table());
        List<String> columns = singleColumnNames(p, svc.schema());
        StreamRunner runner;
        if (p.sortColumn() >= 0) {
            // ORDER BY needs materialization (bounded by LIMIT top-K); collect then emit.
            final String finalSql = sql;
            runner = sink -> {
                for (Object[] r : svc.executor().executeSql(finalSql, svc.schema(), null)) {
                    sink.row(r);
                }
            };
        } else {
            runner = sink -> svc.executor().scan(p.fromMs(), p.toMs(), p.predicate(),
                    row -> sink.row(p.projectRow(row)));
        }
        return new Prepared(columns, runner);
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
