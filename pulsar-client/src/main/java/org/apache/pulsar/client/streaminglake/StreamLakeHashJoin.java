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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A two-phase broadcast hash join for StreamLake scans: the smaller (already heavily pruned) side is
 * built into a hash table keyed by the join column; the other side is streamed and probed, emitting
 * {@code concat(probeRow, buildRow)} for each inner match. This is the "minimize intermediate data
 * reaching the join" operator -- pruning shrinks both inputs first, so the build side fits in memory.
 *
 * <p>The build table is an on-heap multimap here; a production deployment can swap it for an off-heap
 * / NVMe-backed map (e.g. Chronicle Map) behind {@link #addBuildRow} without changing the probe. A
 * {@code maxBuildRows} guard provides simple admission control (fail fast instead of OOM).
 */
public final class StreamLakeHashJoin {

    private final int buildKeyColumn;
    private final long maxBuildRows;
    private final Map<Object, List<Object[]>> table = new HashMap<>();
    private long buildRows;

    public StreamLakeHashJoin(int buildKeyColumn) {
        this(buildKeyColumn, Long.MAX_VALUE);
    }

    public StreamLakeHashJoin(int buildKeyColumn, long maxBuildRows) {
        this.buildKeyColumn = buildKeyColumn;
        this.maxBuildRows = maxBuildRows;
    }

    /** Add a build-side row, indexed by its join key. Null keys never match and are dropped. */
    public void addBuildRow(Object[] row) {
        Object key = row[buildKeyColumn];
        if (key == null) {
            return;
        }
        if (buildRows >= maxBuildRows) {
            throw new IllegalStateException("StreamLake hash-join build side exceeded " + maxBuildRows
                    + " rows; spill to an off-heap/NVMe map or increase the budget");
        }
        table.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        buildRows++;
    }

    public long buildSize() {
        return buildRows;
    }

    /** Inner-join the probe rows against the build table, emitting concat(probeRow, buildRow). */
    public List<Object[]> joinInner(Iterable<Object[]> probeRows, int probeKeyColumn) {
        List<Object[]> out = new ArrayList<>();
        for (Object[] probe : probeRows) {
            Object key = probe[probeKeyColumn];
            if (key == null) {
                continue;
            }
            List<Object[]> matches = table.get(key);
            if (matches == null) {
                continue;
            }
            for (Object[] buildRow : matches) {
                out.add(concat(probe, buildRow));
            }
        }
        return out;
    }

    private static Object[] concat(Object[] a, Object[] b) {
        Object[] r = new Object[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }
}
