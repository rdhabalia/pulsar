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
package org.apache.pulsar.client.streaminglake;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * Builds per-batch pruning stats for the indexed columns of a StreamLake batch: order-preserving
 * min/max plus a low-cardinality exact set (distinct count &lt;= cap) or else a high-cardinality
 * bloom. Cardinality is measured within the batch.
 */
public final class StreamLakeStatsBuilder {

    private StreamLakeStatsBuilder() {
    }

    /**
     * @param indexedColumns   column indexes (into {@code schema}) to compute stats for
     * @param setMaxCardinality distinct-count threshold below which an exact set is kept (else bloom)
     * @param bloomFpp         target bloom false-positive probability for high-cardinality columns
     */
    public static StreamLakeBatchStats build(StreamLakeSchema schema, List<Object[]> rows,
            List<Integer> indexedColumns, int setMaxCardinality, double bloomFpp) {
        Comparator<byte[]> unsigned = Arrays::compareUnsigned;
        List<StreamLakeBatchStats.ColumnStats> out = new ArrayList<>(indexedColumns.size());
        for (int col : indexedColumns) {
            StreamLakeType type = schema.columns().get(col).type();
            TreeSet<byte[]> distinct = new TreeSet<>(unsigned);
            for (Object[] row : rows) {
                Object v = row[col];
                if (v != null) {
                    distinct.add(StreamLakeOrderPreserving.encode(type, v));
                }
            }
            if (distinct.isEmpty()) {
                out.add(new StreamLakeBatchStats.ColumnStats(col, type, null, null, 0, null, null));
                continue;
            }
            byte[] min = distinct.first();
            byte[] max = distinct.last();
            int distinctCount = distinct.size();
            if (distinctCount <= setMaxCardinality) {
                byte[][] set = distinct.toArray(new byte[0][]);
                out.add(new StreamLakeBatchStats.ColumnStats(col, type, min, max, distinctCount, set, null));
            } else {
                byte[] bloom = StreamLakeBloom.build(distinct, bloomFpp).encode();
                out.add(new StreamLakeBatchStats.ColumnStats(col, type, min, max, distinctCount, null, bloom));
            }
        }
        return new StreamLakeBatchStats(out, rows.size());
    }
}
