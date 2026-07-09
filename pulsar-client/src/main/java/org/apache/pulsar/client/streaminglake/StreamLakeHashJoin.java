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
import java.util.Collections;
import java.util.List;

/**
 * A two-phase broadcast hash join for StreamLake scans: the smaller (already heavily pruned) side is
 * built into a {@link StreamLakeJoinTable} keyed by the join column; the other side is streamed and
 * probed, emitting {@code concat(probeRow, buildRow)} for each inner match. This is the "minimize
 * intermediate data reaching the join" operator -- pruning shrinks both inputs first, so the build
 * side is small.
 *
 * <p>The build table is pluggable ({@link OnHeapJoinTable} by default, {@link SpillingJoinTable} for
 * build sides larger than RAM). Callers that late-materialize the probe side use {@link #matches} to
 * probe by key before materializing the full probe row.
 */
public final class StreamLakeHashJoin implements AutoCloseable {

    private final int buildKeyColumn;
    private final StreamLakeJoinTable table;

    public StreamLakeHashJoin(int buildKeyColumn) {
        this(buildKeyColumn, new OnHeapJoinTable(Long.MAX_VALUE));
    }

    public StreamLakeHashJoin(int buildKeyColumn, long maxBuildRows) {
        this(buildKeyColumn, new OnHeapJoinTable(maxBuildRows));
    }

    public StreamLakeHashJoin(int buildKeyColumn, StreamLakeJoinTable table) {
        this.buildKeyColumn = buildKeyColumn;
        this.table = table;
    }

    /** Add a build-side row, indexed by its join key. Null keys never match and are dropped. */
    public void addBuildRow(Object[] row) {
        Object key = row[buildKeyColumn];
        if (key == null) {
            return;
        }
        table.add(key, row);
    }

    public long buildSize() {
        return table.size();
    }

    /** The build rows matching a probe key (empty if none) -- for streaming late-materialized probes. */
    public List<Object[]> matches(Object probeKey) {
        if (probeKey == null) {
            return Collections.emptyList();
        }
        return table.get(probeKey);
    }

    /** Inner-join the (already materialized) probe rows against the build table, emitting concat rows. */
    public List<Object[]> joinInner(Iterable<Object[]> probeRows, int probeKeyColumn) {
        List<Object[]> out = new ArrayList<>();
        for (Object[] probe : probeRows) {
            for (Object[] buildRow : matches(probe[probeKeyColumn])) {
                out.add(concat(probe, buildRow));
            }
        }
        return out;
    }

    /** Concatenate a probe row and a build row into one output row: {@code [probe..., build...]}. */
    public static Object[] concat(Object[] a, Object[] b) {
        Object[] r = new Object[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    @Override
    public void close() {
        table.close();
    }
}
