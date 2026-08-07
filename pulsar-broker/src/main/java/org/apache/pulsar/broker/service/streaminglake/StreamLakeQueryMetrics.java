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

import org.apache.pulsar.common.policies.data.StreamLakeQueryStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-query, broker-side execution counters for a StreamLake query, accumulated by the
 * {@link StreamLakeQueryExecutor} as it prunes, reads and decodes pages. One instance is created per
 * query by the {@link StreamLakeQueryCoordinator} and shared across both sides of a join (so the
 * numbers are the whole-query totals), then serialized to a {@link StreamLakeQueryStats} for the wire.
 *
 * <p>All mutations happen on the single query thread (page reads are prefetched on a pool but counted
 * on the calling thread when awaited), so plain fields are sufficient — no synchronization needed.
 */
public final class StreamLakeQueryMetrics {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeQueryMetrics.class);
    private static final long PROGRESS_LOG_INTERVAL_NANOS = 1_000_000_000L; // at most once/sec

    private long rowsRead;
    private long bytesRead;
    private long pagesRead;
    private long maxPageBytes;
    private long pagesScanned;
    private long pagesKept;
    private long candidateLedgers;
    // Per-tier storage reads for progress logging / a debuggable footer.
    private long segmentsLoaded;
    private long segmentBytes;
    private long pageIndexReads;
    private long pageIndexBytes;
    private long unsegmentedLedgersSkipped;
    private int readConcurrency = 1;

    private String queryTag = "";
    private final long startNanos = System.nanoTime();
    private long lastProgressLogNanos;

    /** A short label (e.g. truncated SQL) so progress/summary log lines can be tied to the query. */
    public void setQueryTag(String queryTag) {
        this.queryTag = queryTag == null ? "" : queryTag;
    }

    /** Count one row decoded from a data page (whether or not it passed the predicate). */
    public void incRowsRead() {
        rowsRead++;
    }

    /** Record one data page read from storage: its Arrow byte size (drives bytesRead + peak buffer). */
    public void recordPageRead(int bytes) {
        bytesRead += bytes;
        pagesRead++;
        if (bytes > maxPageBytes) {
            maxPageBytes = bytes;
        }
    }

    /** Fold a pruner's per-level counters (pages, candidate ledgers, per-tier reads) into the totals. */
    public void addPruneStats(StreamLakePruner.Stats s) {
        pagesScanned += s.pagesScanned;
        pagesKept += s.pagesKept;
        candidateLedgers += s.candidateLedgers;
        segmentsLoaded += s.segmentsLoaded;
        segmentBytes += s.segmentBytes;
        pageIndexReads += s.pageIndexReads;
        pageIndexBytes += s.pageIndexBytes;
        unsegmentedLedgersSkipped += s.unsegmentedLedgersSkipped;
    }

    /** The executor's read-ahead depth (pages held concurrently), for the peak-buffer estimate. */
    public void setReadConcurrency(int readConcurrency) {
        this.readConcurrency = Math.max(1, readConcurrency);
    }

    /**
     * Emit a throttled INFO progress line (at most once/sec unless {@code force}) so a slow query shows
     * what it is reading. {@code liveStats} is the currently-pruning side's cumulative pruner stats (not
     * yet folded in via {@link #addPruneStats}); the data-page counters come from this instance.
     */
    public void logProgress(StreamLakePruner.Stats liveStats, boolean force) {
        long now = System.nanoTime();
        if (!force && now - lastProgressLogNanos < PROGRESS_LOG_INTERVAL_NANOS) {
            return;
        }
        lastProgressLogNanos = now;
        long candLedgers = liveStats != null ? liveStats.candidateLedgers : candidateLedgers;
        long segLoaded = (liveStats != null ? liveStats.segmentsLoaded : 0) + segmentsLoaded;
        long segBytes = (liveStats != null ? liveStats.segmentBytes : 0) + segmentBytes;
        long piReads = (liveStats != null ? liveStats.pageIndexReads : 0) + pageIndexReads;
        long piBytes = (liveStats != null ? liveStats.pageIndexBytes : 0) + pageIndexBytes;
        long unsegSkipped = (liveStats != null ? liveStats.unsegmentedLedgersSkipped : 0)
                + unsegmentedLedgersSkipped;
        log.info("StreamLake query [{}] progress: candidateLedgers={} unsegmentedSkipped={} "
                        + "pageIndex(reads={}, {}) segments(loaded={}, {}) dataPages(read={}, {}) "
                        + "rows={} elapsed={}ms",
                queryTag, candLedgers, unsegSkipped, piReads, human(piBytes), segLoaded, human(segBytes),
                pagesRead, human(bytesRead), rowsRead, (now - startNanos) / 1_000_000);
    }

    /** Emit the final one-line summary of everything this query read (call once at the end). */
    public void logSummary() {
        log.info("StreamLake query [{}] done: candidateLedgers={} unsegmentedSkipped={} "
                        + "pageIndex(reads={}, {}) segments(loaded={}, {}) dataPages(read={}, {}) "
                        + "rowsRead={} elapsed={}ms",
                queryTag, candidateLedgers, unsegmentedLedgersSkipped, pageIndexReads,
                human(pageIndexBytes), segmentsLoaded, human(segmentBytes), pagesRead, human(bytesRead),
                rowsRead, (System.nanoTime() - startNanos) / 1_000_000);
    }

    private static String human(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        if (bytes < 1024L * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        }
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    /** Snapshot the broker-measured counters into a wire {@link StreamLakeQueryStats}. */
    public StreamLakeQueryStats toStats() {
        StreamLakeQueryStats s = new StreamLakeQueryStats();
        s.setRowsRead(rowsRead);
        s.setBytesRead(bytesRead);
        s.setPagesScanned(pagesScanned);
        s.setPagesKept(pagesKept);
        s.setCandidateLedgers(candidateLedgers);
        // Bounded read-ahead high-water: up to readConcurrency pages held at once, each <= maxPageBytes.
        long window = Math.min(readConcurrency, Math.max(1, pagesRead));
        s.setPeakReadBufferBytes(pagesRead == 0 ? 0 : window * maxPageBytes);
        return s;
    }
}
