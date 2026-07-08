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

    /**
     * When true, the broker packs messages into columnar page entries (batched column-major
     * storage) and transcodes them back on read. When false, each entry stays a normal message
     * tagged with its own range (per-entry pruning).
     */
    @Builder.Default
    private boolean batchingEnabled = false;

    /** Target sealed-page size in bytes (default 2 MB). */
    @Builder.Default
    private int pageSizeBytes = 2 * 1024 * 1024;

    /** Rows per granule (sub-page zone-map row group) for in-page granule pruning (default 256). */
    @Builder.Default
    private int granuleSize = 256;

    /**
     * Max distinct values per column per granule stored as an <b>exact set index</b> (ClickHouse
     * {@code set(N)}). When a granule's distinct count for a column is at most this, the page stores
     * the exact set so equality/IN predicates prune the granule with <i>no false positives</i> (even
     * when the value falls inside the granule's min/max). Above it, the granule falls back to
     * min/max + bloom. Default 64.
     */
    @Builder.Default
    private int setMaxCardinality = 64;

    /**
     * Column id to <b>sort rows by within each page</b> so the per-granule marks form a sparse
     * primary index (ClickHouse MergeTree primary key). A predicate on this column then
     * binary-searches the granule range instead of scanning every granule. 0 (default) = unsorted.
     */
    @Builder.Default
    private int sortColumnId = 0;

    /** Max messages packed into one page before it is sealed (default 1000). */
    @Builder.Default
    private int maxPageMessages = 1000;

    /**
     * When true, each page compresses its INT/LONG column blocks with the smallest of several
     * lossless integer codecs (frame-of-reference, delta, dictionary, or raw) chosen per column.
     * Purely a storage/transfer optimization: decoding reproduces the exact values, so scans,
     * pruning and consumer delivery are unchanged. Default false (raw fixed-stride columns).
     */
    @Builder.Default
    private boolean columnCompressionEnabled = false;

    /** Grouping window: a partial page is sealed after this many ms (default 10). */
    @Builder.Default
    private long pageGroupingDelayMs = 10;

    /**
     * When true, the broker maintains a segment-level metadata index: a compaction pass merges each
     * closed ledger's per-page stats into coarse segment summaries (min/max + exact set) stored in a
     * per-topic index-ledger, so scans can skip whole segments before issuing a bookie page prune.
     * Read-side only; default false.
     */
    @Builder.Default
    private boolean segmentIndexEnabled = false;

    /** Pages summarized into one segment when building the segment index (default 1024). */
    @Builder.Default
    private int pagesPerSegment = 1024;

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
