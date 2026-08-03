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

import java.util.function.Function;
import org.apache.pulsar.client.streaminglake.StreamLakeRowCodec;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.rocksdb.RocksIterator;

/**
 * Out-of-core {@code ORDER BY} (no small {@code LIMIT}) via RocksDB, so a sort larger than memory does
 * not OOM the broker (the in-heap {@code List.sort} path does). It scans the (pruned) rows and writes
 * each under an <b>order-preserving</b> key {@code orderKey(sortValue) ‖ seq} (see
 * {@link StreamLakeOrderKeyCodec}); RocksDB keeps them sorted on disk. It then iterates the store in key
 * order — forward for ascending, reverse for descending — and streams each projected row to the sink.
 * Only RocksDB's bounded off-heap cache/memtables are resident, not the whole result.
 *
 * <p>{@code ORDER BY … LIMIT k} stays on the cheaper bounded top-K heap; this operator is for unbounded
 * (or large-limit) sorts.
 */
public final class StreamLakeExternalSort {

    private StreamLakeExternalSort() {
    }

    /**
     * Scan + externally sort by {@code sortColumn} and stream projected rows in order.
     *
     * @param project applied to each full row before it is emitted (SELECT projection)
     */
    public static void sort(StreamLakeQueryExecutor exec, long fromMs, long toMs,
            StreamLakeScanPredicate predicate, int sortColumn, StreamLakeType sortType, boolean descending,
            Function<Object[], Object[]> project, String spillDir, long blockCacheBytes,
            long writeBufferBytes, StreamLakeQueryExecutor.RowConsumer out) throws Exception {
        try (RocksDbStore store = RocksDbStore.open(spillDir, blockCacheBytes, writeBufferBytes)) {
            long[] seq = {0};
            exec.scan(fromMs, toMs, predicate, row -> {
                byte[] okey = StreamLakeOrderKeyCodec.encode(row[sortColumn], sortType);
                byte[] key = new byte[okey.length + 8];
                System.arraycopy(okey, 0, key, 0, okey.length);
                long s = seq[0]++;
                for (int i = 0; i < 8; i++) {
                    key[okey.length + i] = (byte) (s >>> (56 - 8 * i));
                }
                store.put(key, StreamLakeRowCodec.encode(row));
            });

            try (RocksIterator it = store.newIterator()) {
                if (descending) {
                    for (it.seekToLast(); it.isValid(); it.prev()) {
                        out.accept(project.apply(StreamLakeRowCodec.decode(it.value())));
                    }
                } else {
                    for (it.seekToFirst(); it.isValid(); it.next()) {
                        out.accept(project.apply(StreamLakeRowCodec.decode(it.value())));
                    }
                }
            }
        }
    }
}
