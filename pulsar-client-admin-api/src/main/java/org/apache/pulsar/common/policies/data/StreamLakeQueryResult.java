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
package org.apache.pulsar.common.policies.data;

import java.util.ArrayList;
import java.util.List;

/**
 * Result of a StreamLake query submitted through the admin API / REST endpoint: an ordered list of
 * column names plus the result rows (each a list of column values in the same order). Kept
 * JSON-friendly (plain {@code List}s of scalar values) so it round-trips cleanly through the
 * broker REST endpoint and the {@code pulsar-admin} client.
 */
public class StreamLakeQueryResult {

    private List<String> columns = new ArrayList<>();
    private List<List<Object>> rows = new ArrayList<>();
    private long latencyMs;

    public StreamLakeQueryResult() {
    }

    public StreamLakeQueryResult(List<String> columns, List<List<Object>> rows, long latencyMs) {
        this.columns = columns;
        this.rows = rows;
        this.latencyMs = latencyMs;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public void setRows(List<List<Object>> rows) {
        this.rows = rows;
    }

    /** Number of result rows. */
    public int getRowCount() {
        return rows == null ? 0 : rows.size();
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public void setLatencyMs(long latencyMs) {
        this.latencyMs = latencyMs;
    }
}
