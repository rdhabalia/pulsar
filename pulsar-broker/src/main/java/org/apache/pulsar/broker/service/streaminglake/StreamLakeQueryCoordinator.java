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
 * Broker-side query coordinator: turns a SQL string into a {@link StreamLakeQueryResult} by planning it
 * with {@link StreamLakeSqlPlanner} and executing it against one or two per-table
 * {@link StreamLakeQueryService}s (single-table scan or inner equi-join). Table names in the SQL are
 * resolved to their query service via the supplied resolver (which maps a table to its topic in the
 * query's namespace). This is the single entry point the REST endpoint calls, so no client or CLI ever
 * assembles the pruner/executor itself.
 */
public final class StreamLakeQueryCoordinator {

    private final Function<String, StreamLakeQueryService> serviceByTable;

    public StreamLakeQueryCoordinator(Function<String, StreamLakeQueryService> serviceByTable) {
        this.serviceByTable = serviceByTable;
    }

    /** Plan and execute {@code sql}, returning the column header + rows (+ latency). */
    public StreamLakeQueryResult executeSql(String sql) throws Exception {
        long t0 = System.nanoTime();
        Function<String, StreamLakeSchema> schemas = table -> {
            StreamLakeQueryService s = serviceByTable.apply(table);
            return s == null ? null : s.schema();
        };
        StreamLakeSqlPlanner.Planned planned = StreamLakeSqlPlanner.planStatement(sql, schemas, t -> null);

        if (planned.isJoin()) {
            StreamLakeSqlPlanner.JoinPlan jp = planned.join();
            StreamLakeQueryService left = require(jp.leftTable());
            StreamLakeQueryService right = require(jp.rightTable());
            List<Object[]> concat = left.executor().scanInnerJoin(0, Long.MAX_VALUE,
                    jp.leftPredicate(), jp.leftKey(), right.executor(), jp.rightPredicate(), jp.rightKey(),
                    new OnHeapJoinTable(Long.MAX_VALUE));
            return result(jp.columnNames(), jp.combine(concat), t0);
        }

        StreamLakeSqlPlanner.Plan p = planned.single();
        StreamLakeQueryService svc = require(p.table());
        List<Object[]> rows = svc.executor().executeSql(sql, svc.schema(), null);
        return result(singleColumnNames(p, svc.schema()), rows, t0);
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

    private static StreamLakeQueryResult result(List<String> columns, List<Object[]> rows, long startNanos) {
        List<List<Object>> jsonRows = new ArrayList<>(rows.size());
        for (Object[] r : rows) {
            jsonRows.add(new ArrayList<>(Arrays.asList(r)));
        }
        long latencyMs = (System.nanoTime() - startNanos) / 1_000_000;
        return new StreamLakeQueryResult(columns, jsonRows, latencyMs);
    }
}
