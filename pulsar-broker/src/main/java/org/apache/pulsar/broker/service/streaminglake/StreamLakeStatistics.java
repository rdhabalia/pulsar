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

import java.util.List;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;

/**
 * Cost statistics for the query planner: cheap, <b>metadata-only</b> estimates of how much data a scan
 * touches after pruning, so the coordinator can pick a join strategy (broadcast vs partitioned) and
 * size its build table / partition count without reading any data pages.
 *
 * <p>The estimate is derived from the pruner's surviving-page count (date &rarr; segment &rarr; page
 * pruning) times a configured average page shape ({@code rowsPerPage}, {@code pageBytes}). Page count is
 * exact (the pruner already computed it); rows/bytes are approximations good enough for a relative
 * "which side is smaller / does it fit memory" decision. Distinct-key cardinality and heavy-hitter
 * (skew) statistics are a planned extension (per-column HLL + top-K sketches merged at segment build).
 */
public final class StreamLakeStatistics {

    /** A pruned-scan size estimate for one table (one side of a query). */
    public static final class Estimate {
        /** Surviving pages after pruning (exact). */
        public final long pages;
        /** Distinct data ledgers the surviving pages span. */
        public final long ledgers;
        /** Estimated surviving rows ({@code pages * rowsPerPage}). */
        public final long rows;
        /** Estimated surviving bytes ({@code pages * pageBytes}). */
        public final long bytes;

        public Estimate(long pages, long ledgers, long rows, long bytes) {
            this.pages = pages;
            this.ledgers = ledgers;
            this.rows = rows;
            this.bytes = bytes;
        }

        @Override
        public String toString() {
            return "pages=" + pages + " ledgers=" + ledgers + " rows~" + rows + " bytes~" + bytes;
        }
    }

    private final long rowsPerPage;
    private final long pageBytes;

    public StreamLakeStatistics(long rowsPerPage, long pageBytes) {
        this.rowsPerPage = Math.max(1, rowsPerPage);
        this.pageBytes = Math.max(1, pageBytes);
    }

    /**
     * Estimate the pruned size of {@code predicate} over {@code [fromMs, toMs]} using {@code pruner}
     * (metadata-only). Returns the surviving pages count and derived row/byte estimates.
     */
    public Estimate estimate(StreamLakePruner pruner, long fromMs, long toMs,
            StreamLakeScanPredicate predicate) throws Exception {
        StreamLakePruner.Stats s = new StreamLakePruner.Stats();
        List<StreamLakePruner.PagePointer> pages = pruner.prune(fromMs, toMs, predicate, s);
        long pageCount = pages.size();
        long ledgers = pages.stream().mapToLong(p -> p.ledgerId).distinct().count();
        return new Estimate(pageCount, ledgers, pageCount * rowsPerPage, pageCount * pageBytes);
    }
}
