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

import java.util.List;
import org.apache.pulsar.broker.service.streaminglake.StreamLakeSqlPlanner.Agg;
import org.apache.pulsar.broker.service.streaminglake.StreamLakeSqlPlanner.AggFunc;
import org.apache.pulsar.broker.service.streaminglake.StreamLakeSqlPlanner.GroupByPlan;
import org.apache.pulsar.client.streaminglake.StreamLakeRowCodec;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.rocksdb.RocksIterator;

/**
 * Out-of-core {@code GROUP BY} aggregation via RocksDB, so grouping by a high-cardinality key does not
 * OOM the broker. It scans the (pruned) rows and, per row, read-modify-writes the group's aggregate
 * state in RocksDB keyed by the group-key tuple (encoded with {@link StreamLakeRowCodec}, so equal
 * tuples collide and groups come out sorted). It then iterates the store, finalizes each group
 * ({@code AVG = sum/count}, ...), and streams one result row per group in SELECT order.
 *
 * <p>Aggregate state is stored as an {@code Object[]} (flattened across the query's aggregates) encoded
 * with {@link StreamLakeRowCodec}: {@code COUNT/SUM/MIN/MAX} use one slot, {@code AVG} uses two
 * (sum, count). Read-modify-write per row is simple and correct; a RocksDB merge operator is a future
 * optimization.
 */
public final class StreamLakeGroupBy {

    private StreamLakeGroupBy() {
    }

    public static void aggregate(StreamLakeQueryExecutor exec, GroupByPlan plan, String spillDir,
            long blockCacheBytes, long writeBufferBytes, StreamLakeQueryExecutor.RowConsumer out)
            throws Exception {
        int[] groupCols = plan.groupCols();
        List<Agg> aggs = plan.aggs();
        int[] offsets = new int[aggs.size()];
        int width = 0;
        for (int i = 0; i < aggs.size(); i++) {
            offsets[i] = width;
            width += slots(aggs.get(i).func());
        }
        int[] outputKind = plan.outputKind();

        try (RocksDbStore store = RocksDbStore.open(spillDir, blockCacheBytes, writeBufferBytes)) {
            final int stateWidth = width;
            exec.scan(plan.fromMs(), plan.toMs(), plan.predicate(), row -> {
                Object[] key = new Object[groupCols.length];
                for (int i = 0; i < groupCols.length; i++) {
                    key[i] = row[groupCols[i]];
                }
                byte[] keyBytes = StreamLakeRowCodec.encode(key);
                byte[] cur = store.get(keyBytes);
                Object[] state = cur == null ? initState(aggs, stateWidth)
                        : StreamLakeRowCodec.decode(cur);
                for (int a = 0; a < aggs.size(); a++) {
                    update(state, offsets[a], aggs.get(a), row);
                }
                store.put(keyBytes, StreamLakeRowCodec.encode(state));
            });

            try (RocksIterator it = store.newIterator()) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    Object[] groupKey = StreamLakeRowCodec.decode(it.key());
                    Object[] state = StreamLakeRowCodec.decode(it.value());
                    Object[] outRow = new Object[outputKind.length];
                    for (int c = 0; c < outputKind.length; c++) {
                        if (outputKind[c] >= 0) {
                            outRow[c] = groupKey[outputKind[c]];
                        } else {
                            int aggIdx = -outputKind[c] - 1;
                            outRow[c] = finalize(aggs.get(aggIdx), state, offsets[aggIdx]);
                        }
                    }
                    out.accept(outRow);
                }
            }
        }
    }

    private static int slots(AggFunc func) {
        return func == AggFunc.AVG ? 2 : 1;
    }

    private static Object[] initState(List<Agg> aggs, int width) {
        Object[] state = new Object[width];
        int off = 0;
        for (Agg agg : aggs) {
            switch (agg.func()) {
                case COUNT:
                    state[off] = 0L;
                    break;
                case AVG:
                    state[off] = 0.0d;
                    state[off + 1] = 0L;
                    break;
                default: // SUM / MIN / MAX start empty (null) until the first non-null value
                    state[off] = null;
            }
            off += slots(agg.func());
        }
        return state;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void update(Object[] state, int off, Agg agg, Object[] row) {
        Object v = agg.columnIndex() < 0 ? null : row[agg.columnIndex()];
        switch (agg.func()) {
            case COUNT:
                if (agg.columnIndex() < 0 || v != null) { // COUNT(*) counts all; COUNT(col) skips nulls
                    state[off] = ((Long) state[off]) + 1;
                }
                return;
            case SUM:
                if (v != null) {
                    state[off] = add(state[off], v, agg.type());
                }
                return;
            case MIN:
                if (v != null && (state[off] == null || ((Comparable) v).compareTo(state[off]) < 0)) {
                    state[off] = v;
                }
                return;
            case MAX:
                if (v != null && (state[off] == null || ((Comparable) v).compareTo(state[off]) > 0)) {
                    state[off] = v;
                }
                return;
            case AVG:
                if (v != null) {
                    state[off] = ((Double) state[off]) + ((Number) v).doubleValue();
                    state[off + 1] = ((Long) state[off + 1]) + 1;
                }
                return;
            default:
                throw new IllegalStateException("Unsupported aggregate: " + agg.func());
        }
    }

    private static Object add(Object sum, Object value, StreamLakeType type) {
        if (type == StreamLakeType.DOUBLE) {
            return (sum == null ? 0.0d : ((Number) sum).doubleValue()) + ((Number) value).doubleValue();
        }
        return (sum == null ? 0L : ((Number) sum).longValue()) + ((Number) value).longValue();
    }

    private static Object finalize(Agg agg, Object[] state, int off) {
        if (agg.func() == AggFunc.AVG) {
            long count = (Long) state[off + 1];
            return count == 0 ? null : ((Double) state[off]) / count;
        }
        return state[off]; // COUNT (Long), SUM (Number/null), MIN/MAX (typed/null)
    }
}
