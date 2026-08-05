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

    private long rowsRead;
    private long bytesRead;
    private long pagesRead;
    private long maxPageBytes;
    private long pagesScanned;
    private long pagesKept;
    private long candidateLedgers;
    private int readConcurrency = 1;

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

    /** Fold a pruner's per-level counters (pages scanned/kept, candidate ledgers) into the totals. */
    public void addPruneStats(StreamLakePruner.Stats s) {
        pagesScanned += s.pagesScanned;
        pagesKept += s.pagesKept;
        candidateLedgers += s.candidateLedgers;
    }

    /** The executor's read-ahead depth (pages held concurrently), for the peak-buffer estimate. */
    public void setReadConcurrency(int readConcurrency) {
        this.readConcurrency = Math.max(1, readConcurrency);
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
