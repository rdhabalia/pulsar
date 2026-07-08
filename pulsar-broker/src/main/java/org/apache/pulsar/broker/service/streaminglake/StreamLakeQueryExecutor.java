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

import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.client.streaminglake.StreamLakeArrowBatchDecoder;
import org.apache.pulsar.client.streaminglake.StreamLakeHashJoin;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;
import org.apache.pulsar.client.streaminglake.StreamLakeTopK;

/**
 * The StreamLake query execution engine (redesign phase F): composes the read-side primitives into a
 * runnable scan/join/top-K over a topic. {@link StreamLakePruner} yields the surviving pages, a
 * {@link PageReader} returns each page's Arrow batch, the decoder materializes rows, and the
 * {@link StreamLakeScanPredicate} row-filters them exactly (pruning is conservative). Scans compose
 * with {@link StreamLakeHashJoin} and {@link StreamLakeTopK}.
 *
 * <p>This is the executor a SQL frontend (e.g. an Apache Calcite adapter) targets: Calcite parses SQL
 * and pushes filters/projection down into a {@code StreamLakeScanPredicate} + this executor's calls.
 * The {@link PageReader} is the single broker/storage seam -- in a broker it reads the managed-ledger
 * entry and strips the payload framing to the Arrow bytes.
 */
public class StreamLakeQueryExecutor {

    /** Reads the raw Arrow batch bytes for a pruned page (one data-ledger entry). */
    public interface PageReader {
        byte[] readArrowBatch(long ledgerId, long entryId) throws Exception;
    }

    private final StreamLakePruner pruner;
    private final PageReader pageReader;

    public StreamLakeQueryExecutor(StreamLakePruner pruner, PageReader pageReader) {
        this.pruner = pruner;
        this.pageReader = pageReader;
    }

    /** Scan a topic: prune to candidate pages, read them, and exactly row-filter by the predicate. */
    public List<Object[]> scan(long fromMs, long toMs, StreamLakeScanPredicate predicate) throws Exception {
        List<StreamLakePruner.PagePointer> pages = pruner.prune(fromMs, toMs, predicate);
        List<Object[]> rows = new ArrayList<>();
        try (StreamLakeArrowBatchDecoder decoder = new StreamLakeArrowBatchDecoder()) {
            for (StreamLakePruner.PagePointer p : pages) {
                byte[] arrow = pageReader.readArrowBatch(p.ledgerId, p.entryId);
                for (Object[] row : decoder.decodeRows(arrow)) {
                    if (predicate.matchesRow(row)) {
                        rows.add(row);
                    }
                }
            }
        }
        return rows;
    }

    /** Scan and keep only the top-K rows by a sort column (ORDER BY ... LIMIT). */
    public List<Object[]> scanTopK(long fromMs, long toMs, StreamLakeScanPredicate predicate,
            int k, int sortColumn, boolean descending) throws Exception {
        StreamLakeTopK topK = new StreamLakeTopK(k, sortColumn, descending);
        topK.offerAll(scan(fromMs, toMs, predicate));
        return topK.results();
    }

    /**
     * Inner-join two scanned sides on a join column. The build side (smaller, already pruned) is
     * materialized into the hash table; the probe side streams. Emits concat(probeRow, buildRow).
     */
    public List<Object[]> scanInnerJoin(
            long fromMs, long toMs,
            StreamLakeScanPredicate buildPredicate, int buildKeyColumn,
            StreamLakeQueryExecutor probeSide, StreamLakeScanPredicate probePredicate, int probeKeyColumn,
            long maxBuildRows) throws Exception {
        StreamLakeHashJoin join = new StreamLakeHashJoin(buildKeyColumn, maxBuildRows);
        for (Object[] buildRow : scan(fromMs, toMs, buildPredicate)) {
            join.addBuildRow(buildRow);
        }
        return join.joinInner(probeSide.scan(fromMs, toMs, probePredicate), probeKeyColumn);
    }
}
