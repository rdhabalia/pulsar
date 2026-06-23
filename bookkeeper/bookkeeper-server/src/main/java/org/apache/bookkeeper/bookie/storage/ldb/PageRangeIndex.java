/*
 *
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
 *
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Streaming Lake page-range index: a new RocksDB column family ("page-ranges")
 * mapping {@code (ledgerId, entryId) -> opaque column-range blob}. Modeled on
 * {@link EntryLocationIndex} but storing variable-length range metadata instead of a
 * fixed-width location.
 *
 * <p>The bookie uses this index to answer page-prune requests
 * ({@link #giveIndexPages}) without ever decoding the page payload or understanding
 * the schema — it only compares order-preserving bytes via {@link PageRangeCodec}.
 */
public class PageRangeIndex implements Closeable {

    private final KeyValueStorage pageRangesDb;

    public PageRangeIndex(ServerConfiguration conf, KeyValueStorageFactory storageFactory, String basePath,
                          StatsLogger stats) throws IOException {
        this.pageRangesDb = storageFactory.newKeyValueStorage(basePath, "page-ranges",
                DbConfigType.EntryLocation, conf);
    }

    /** Index a sealed page's range blob under (ledgerId, entryId). */
    public void addPageRanges(long ledgerId, long entryId, byte[] rangeBlob) throws IOException {
        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        try {
            pageRangesDb.put(key.array, rangeBlob);
        } finally {
            key.recycle();
        }
    }

    /** Fetch the range blob for a page, or null if absent. */
    public byte[] getPageRanges(long ledgerId, long entryId) throws IOException {
        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        try {
            return pageRangesDb.get(key.array);
        } finally {
            key.recycle();
        }
    }

    /**
     * The bookie page-prune API. Iterate the index for entries in
     * [startEntryId, endEntryId] within a ledger and return the entryIds whose ranges
     * could satisfy the predicate blob. Pure byte comparison; schema-agnostic.
     */
    public List<Long> giveIndexPages(long ledgerId, long startEntryId, long endEntryId, byte[] predicateBlob)
            throws IOException {
        List<Long> matches = new ArrayList<>();
        LongPairWrapper firstKey = LongPairWrapper.get(ledgerId, startEntryId);
        LongPairWrapper lastKey = LongPairWrapper.get(ledgerId, endEntryId + 1); // exclusive upper bound
        try (CloseableIterator<byte[]> it = pageRangesDb.keys(firstKey.array, lastKey.array)) {
            while (it.hasNext()) {
                byte[] keyBytes = it.next();
                long entryId = getLong(keyBytes, 8);
                byte[] pageBlob = pageRangesDb.get(keyBytes);
                if (pageBlob != null && PageRangeCodec.pageCouldMatch(pageBlob, predicateBlob)) {
                    matches.add(entryId);
                }
            }
        } finally {
            firstKey.recycle();
            lastKey.recycle();
        }
        return matches;
    }

    public void delete(long ledgerId) throws IOException {
        LongPairWrapper firstKey = LongPairWrapper.get(ledgerId, 0);
        LongPairWrapper lastKey = LongPairWrapper.get(ledgerId + 1, 0);
        try (KeyValueStorage.Batch batch = pageRangesDb.newBatch()) {
            batch.deleteRange(firstKey.array, lastKey.array);
            batch.flush();
        } finally {
            firstKey.recycle();
            lastKey.recycle();
        }
    }

    @Override
    public void close() throws IOException {
        pageRangesDb.close();
    }

    private static long getLong(byte[] array, int index) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (array[index + i] & 0xFFL);
        }
        return v;
    }
}
