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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Merges several per-batch {@link StreamLakeBatchStats} footers into one coarser summary: the input
 * for building a <b>segment</b> (the read-side layer above the per-page footers). Per indexed column
 * it unions the order-preserving min/max (always -&gt; range skip), unions the low-cardinality exact
 * sets when their union stays within the cap (exact membership), promotes a slightly-larger union to a
 * segment-level bloom, and otherwise keeps only min/max for genuinely high-cardinality columns (whose
 * precise equality pruning stays at the per-page bloom). No false negatives.
 */
public final class StreamLakeStatsMerger {

    private static final Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;

    private StreamLakeStatsMerger() {
    }

    public static StreamLakeBatchStats merge(List<StreamLakeBatchStats> parts, int setCap, double bloomFpp) {
        // group each column's per-part stats, preserving first-seen column order
        Map<Integer, List<StreamLakeBatchStats.ColumnStats>> byColumn = new LinkedHashMap<>();
        for (StreamLakeBatchStats part : parts) {
            for (StreamLakeBatchStats.ColumnStats cs : part.columns()) {
                byColumn.computeIfAbsent(cs.columnIndex(), k -> new ArrayList<>()).add(cs);
            }
        }

        List<StreamLakeBatchStats.ColumnStats> merged = new ArrayList<>(byColumn.size());
        for (Map.Entry<Integer, List<StreamLakeBatchStats.ColumnStats>> e : byColumn.entrySet()) {
            merged.add(mergeColumn(e.getKey(), e.getValue(), setCap, bloomFpp));
        }
        return new StreamLakeBatchStats(merged);
    }

    private static StreamLakeBatchStats.ColumnStats mergeColumn(int columnIndex,
            List<StreamLakeBatchStats.ColumnStats> parts, int setCap, double bloomFpp) {
        StreamLakeType type = parts.get(0).type();
        byte[] min = null;
        byte[] max = null;
        boolean allHaveExactSet = true; // among parts that actually carry values
        boolean anyValues = false;
        TreeSet<byte[]> union = new TreeSet<>(UNSIGNED);

        for (StreamLakeBatchStats.ColumnStats cs : parts) {
            if (cs.min == null) {
                continue; // an all-null batch for this column contributes no constraint
            }
            anyValues = true;
            min = (min == null || UNSIGNED.compare(cs.min, min) < 0) ? cs.min : min;
            max = (max == null || UNSIGNED.compare(cs.max, max) > 0) ? cs.max : max;
            if (cs.exactSet != null) {
                union.addAll(Arrays.asList(cs.exactSet));
            } else {
                allHaveExactSet = false; // a high-cardinality part -> segment can't rebuild a set
            }
        }

        if (!anyValues) {
            return new StreamLakeBatchStats.ColumnStats(columnIndex, type, null, null, 0, null, null);
        }
        if (allHaveExactSet) {
            if (union.size() <= setCap) {
                byte[][] set = union.toArray(new byte[0][]);
                return new StreamLakeBatchStats.ColumnStats(columnIndex, type, min, max, union.size(), set, null);
            }
            byte[] bloom = StreamLakeBloom.build(union, bloomFpp).encode();
            return new StreamLakeBatchStats.ColumnStats(columnIndex, type, min, max, union.size(), null, bloom);
        }
        // high-cardinality: keep only min/max at the segment; per-page blooms handle equality.
        return new StreamLakeBatchStats.ColumnStats(columnIndex, type, min, max, -1, null, null);
    }
}
