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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-topic Streaming Lake configuration.
 *
 * <p>When {@code enabled}, the broker batches messages into columnar pages, computes
 * order-preserving min/max ranges for the {@code indexedColumns}, and stores those
 * ranges in the BookKeeper page-range index so scans can be pruned at the bookie.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StreamingLakeConfig {

    /** Whether Streaming Lake (columnar pages + range index) is enabled for the topic. */
    @Builder.Default
    private boolean enabled = false;

    /** Target sealed-page size in bytes (default 2 MB). */
    @Builder.Default
    private int pageSizeBytes = 2 * 1024 * 1024;

    /** Columns for which the broker emits min/max ranges into the page-range index. */
    @Builder.Default
    private List<IndexedColumn> indexedColumns = new ArrayList<>();

    /**
     * A schema column that participates in range pruning. {@code columnId} is stable
     * for the lifetime of the topic; query execution uses ids, never names.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IndexedColumn {
        private int columnId;
        private String name;
        /** One of INT, LONG, STRING (matches the bookie-side order-preserving encoding). */
        private String type;
    }
}
