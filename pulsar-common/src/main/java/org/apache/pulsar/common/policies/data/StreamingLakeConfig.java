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
 * <p>When {@code enabled}, the producer client encodes each batch as a columnar (Apache Arrow)
 * payload with a trailing per-column stats footer; the broker slices that footer into a shared
 * page-index ledger, a compaction pass merges page stats into segment summaries, and the query tier
 * prunes date -&gt; segment -&gt; page before reading. The {@code indexedColumns} / {@code columns}
 * define which columns emit pruning stats.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StreamingLakeConfig {

    /** Whether Streaming Lake (client columnar pages + metadata prune index) is enabled for the topic. */
    @Builder.Default
    private boolean enabled = false;

    /**
     * When true, the <b>client</b> encodes each batch as a columnar (Arrow) payload with a trailing
     * stats footer, and the broker only slices that footer into the shared page-index ledger (it does
     * not batch, encode, or compute ranges itself). This is the StreamLake write path. Default false.
     */
    @Builder.Default
    private boolean clientColumnarEnabled = false;

    /**
     * Max distinct values per column stored as an <b>exact set index</b> (ClickHouse {@code set(N)}).
     * When a batch's distinct count for a column is at most this, the footer stores the exact set so
     * equality/IN predicates prune with <i>no false positives</i> (even when the value falls inside
     * the min/max). Above it, the column falls back to min/max + bloom. Default 64.
     */
    @Builder.Default
    private int setMaxCardinality = 64;

    /** Pages summarized into one segment when building the segment index (default 1024). */
    @Builder.Default
    private int pagesPerSegment = 1024;

    /**
     * Per-column size cap (bytes) for a segment's per-page stats array. When a column's per-page array
     * (min/max, plus per-page bloom for text) would exceed this, that column collapses to a single
     * whole-segment stat (union min/max + a union bloom when the pages were low-cardinality). Keeps a
     * small text column (e.g. {@code name}) per-page while a high-cardinality one (e.g. {@code email})
     * stays compact. Default 2 MiB.
     */
    @Builder.Default
    private long segmentColumnMaxBytes = 2L * 1024 * 1024;

    /**
     * Max entries (page footers) per page-index ledger before it rolls, so one page-index ledger holds
     * <b>many whole data ledgers</b> (not one per data ledger). The roll is taken at a data-ledger
     * boundary once this threshold is crossed, so each data ledger's footers stay contiguous in one
     * ledger (a single {@code (piLedgerId, start, end)} range). Default 1,000,000.
     */
    @Builder.Default
    private int pageIndexMaxEntriesPerLedger = 1_000_000;

    /** Max entries per segment ledger before it rolls (holds many data ledgers' segments). Default 200,000. */
    @Builder.Default
    private int segmentMaxEntriesPerLedger = 200_000;

    /** Concurrency for reading pruned pages (and segment/page-index ranges) during a query. Default 16. */
    @Builder.Default
    private int queryReadConcurrency = 16;

    /**
     * Planner cost-estimate: assumed rows per page, used to turn the pruner's surviving-page count into
     * a row-count estimate for join-strategy selection. Default 1,000 (≈ a 1 MiB columnar page).
     */
    @Builder.Default
    private long estimatedRowsPerPage = 1000;

    /**
     * Planner cost-estimate: assumed bytes per page, used to turn the pruner's surviving-page count into
     * a byte estimate for join-strategy selection (does this side fit the build memory budget?).
     * Default 1 MiB.
     */
    @Builder.Default
    private long estimatedPageBytes = 1L << 20;

    /**
     * When true, a closed data ledger's segment is built <b>asynchronously off the owning broker</b>:
     * the broker publishes a {@code {dataTopic, dataLedgerId}} build request to a per-namespace system
     * topic ({@code __streamlake_segment_build}) and a StreamLake consumer on a failover subscription
     * builds the segment and acks. When false (default) the owning broker builds inline on its shared
     * executor (still off the producer ack path). The system-topic path decouples build load from the
     * pub-sub broker and lets a dedicated query/builder tier own segment creation.
     */
    @Builder.Default
    private boolean asyncSegmentBuildViaSystemTopic = false;

    /**
     * Max segments held resident per topic. Segments are loaded on demand from their catalog offset and
     * cached in a bounded LRU, so query-tier memory is O(cache) rather than O(all data ledgers).
     * Default 512.
     */
    @Builder.Default
    private int segmentCacheMaxEntries = 512;

    /**
     * Replication of the shared page-index ledger (hot write path): a modest RF-3 by default. Kept
     * lower than the segment RF because it is written on every batch; isolated onto the metadata
     * bookie pool (see {@code metadataBookieAffinityGroup}).
     */
    @Builder.Default
    private int pageIndexEnsembleSize = 3;
    @Builder.Default
    private int pageIndexWriteQuorum = 3;
    @Builder.Default
    private int pageIndexAckQuorum = 2;

    /**
     * Replication of the segment ledgers (immutable, read-heavy pruning tier): higher RF so pruning
     * reads scale horizontally, plus a query-tier cache and object-storage offload. Reject an
     * ensemble far above the bookie count -- prefer caching/offload over RF for read scaling.
     */
    @Builder.Default
    private int segmentEnsembleSize = 5;
    @Builder.Default
    private int segmentWriteQuorum = 5;
    @Builder.Default
    private int segmentAckQuorum = 3;

    /** Bookie affinity/rackaware group isolating StreamLake metadata ledgers off the pub-sub pool. */
    @Builder.Default
    private String metadataBookieAffinityGroup = "";

    /** When true, this broker acts as a StreamLake query-executor (segment build + query engine). */
    @Builder.Default
    private boolean queryExecutorEnabled = false;

    /** When true, immutable cold segments are offloaded to object storage via Pulsar's offloaders. */
    @Builder.Default
    private boolean segmentOffloadEnabled = false;

    /**
     * Inner-join build-side admission guard: the max rows the hash-join build side may hold before it
     * fails fast (rather than OOM). Raise it, or enable {@code joinOffHeapEnabled}, for larger builds.
     */
    @Builder.Default
    private long joinMaxBuildRows = 5_000_000L;

    /**
     * When true, the hash-join build table spills row bytes to a file (only a small key index stays
     * on-heap) instead of holding all rows in the JVM heap — for build sides larger than RAM. Default
     * false: pruning is expected to keep the build side small enough for the fast on-heap table.
     */
    @Builder.Default
    private boolean joinOffHeapEnabled = false;

    /**
     * Broker-local directory for hash-join spill files when {@code joinOffHeapEnabled}. Empty (default)
     * uses the JVM temp dir; point it at fast local NVMe for large builds. Files are deleted after the
     * join completes.
     */
    @Builder.Default
    private String joinSpillDir = "";

    /**
     * Join-strategy selection threshold (bytes): if the smaller pruned side's estimated size is within
     * this budget it is broadcast (built in one hash table, streaming the other side); otherwise the
     * planner switches to a <b>partitioned (Grace) hash join</b> that hash-partitions BOTH sides to disk
     * and joins partition-by-partition (bounded memory). Also sets the per-partition target, so the
     * partition count is {@code ceil(buildBytes / budget)}. Default 256 MiB.
     */
    @Builder.Default
    private long joinBuildMemoryBudget = 256L * 1024 * 1024;

    /** Join execution strategies. AUTO selects by cost; the others force a specific operator. */
    public enum JoinStrategy { AUTO, BROADCAST, GRACE, ROCKSDB }

    /**
     * Force a specific join strategy (mainly for testing / overrides). {@code AUTO} (default) chooses by
     * cost: build the smaller pruned side, BROADCAST it if it fits {@code joinBuildMemoryBudget}, else
     * fall back to a large-build operator ({@code GRACE} partitioned join, or the RocksDB broadcast
     * table when {@code joinLargeBuildUsesRocksDb}). {@code BROADCAST}/{@code GRACE}/{@code ROCKSDB}
     * force that operator regardless of the estimate.
     */
    @Builder.Default
    private JoinStrategy joinStrategy = JoinStrategy.AUTO;

    /**
     * Under {@code joinStrategy=AUTO}, when the build side exceeds the budget: false (default) uses the
     * Grace partitioned join; true builds the side into a RocksDB table instead (disk keys+values).
     */
    @Builder.Default
    private boolean joinLargeBuildUsesRocksDb = false;

    /** Off-heap RocksDB block-cache cap for out-of-core operators (join/sort/group-by). Default 128 MiB. */
    @Builder.Default
    private long rocksdbBlockCacheBytes = 128L * 1024 * 1024;

    /** Off-heap RocksDB write-buffer (memtable) size for out-of-core operators. Default 64 MiB. */
    @Builder.Default
    private long rocksdbWriteBufferBytes = 64L * 1024 * 1024;

    /**
     * Upper bound on the number of on-disk partitions the Grace hash join creates (it opens 2N spill
     * files at once). The computed {@code ceil(buildBytes/budget)} is capped to this. Default 256.
     */
    @Builder.Default
    private int joinMaxPartitions = 256;

    /**
     * Runaway-query guard: if the estimated result-row count exceeds this, the query is rejected before
     * execution (e.g. a many-to-many join on a hot key whose output is quadratic). 0 (default) disables
     * the guard.
     */
    @Builder.Default
    private long runawayResultRows = 0;

    /** Columns for which the broker emits min/max ranges into the page-range index. */
    @Builder.Default
    private List<IndexedColumn> indexedColumns = new ArrayList<>();

    /**
     * The full ordered StreamLake table schema the client encodes into Arrow batches (redesign:
     * client-side columnar encoding). Column index = position in this list; the per-batch stats
     * footer and segment index reference columns by that index. A column with {@code indexed=true}
     * emits per-batch pruning stats (min/max + exact set or bloom).
     */
    @Builder.Default
    private List<SchemaColumn> columns = new ArrayList<>();

    /** Target bloom false-positive probability for high-cardinality indexed columns (default 0.01). */
    @Builder.Default
    private double bloomFpp = 0.01;

    /** StreamLake wire/format version, for forward compatibility (default 1). */
    @Builder.Default
    private int formatVersion = 1;

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

    /**
     * A column in the full StreamLake table schema. {@code columnId} is stable for the topic's
     * lifetime; {@code type} is a StreamLake logical type name (INT32, INT64, DOUBLE, BOOLEAN,
     * STRING, BYTES); {@code indexed} marks columns that emit per-batch pruning stats.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaColumn {
        private int columnId;
        private String name;
        private String type;
        private boolean indexed;
    }
}
