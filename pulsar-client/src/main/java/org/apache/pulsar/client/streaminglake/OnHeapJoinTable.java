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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * On-heap build table: a plain {@code HashMap<key, List<row>>}. The fast default — pruning is expected
 * to keep the build side small enough to fit in the JVM heap. {@code maxRows} is an admission guard
 * that fails fast (rather than OOM) so the caller can switch to {@link SpillingJoinTable}.
 */
public final class OnHeapJoinTable implements StreamLakeJoinTable {

    private final Map<Object, List<Object[]>> table = new HashMap<>();
    private final long maxRows;
    private long rows;

    public OnHeapJoinTable(long maxRows) {
        this.maxRows = maxRows;
    }

    @Override
    public void add(Object key, Object[] row) {
        if (rows >= maxRows) {
            throw new IllegalStateException("StreamLake hash-join build side exceeded " + maxRows
                    + " rows; enable off-heap spilling (joinOffHeapEnabled) or raise joinMaxBuildRows");
        }
        table.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        rows++;
    }

    @Override
    public List<Object[]> get(Object key) {
        List<Object[]> matches = table.get(key);
        return matches == null ? Collections.emptyList() : matches;
    }

    @Override
    public long size() {
        return rows;
    }

    @Override
    public void close() {
        table.clear();
    }
}
