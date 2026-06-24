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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;

/**
 * Unit tests for the bookie-side Streaming Lake page pruning: the RocksDB
 * {@code ledger-page-index} stores per-entry opaque column ranges, and
 * {@link PageRangeIndex#giveIndexPages} returns the entryIds whose ranges could satisfy
 * a predicate -- using pure order-preserving byte comparison, with no schema knowledge.
 */
public class PageRangeIndexTest {

    private final ServerConfiguration conf = new ServerConfiguration();

    private static final short DEPT = 2;
    private static final short SALARY = 3;

    /** Order-preserving int encoding (the same the broker uses for ranges). */
    private static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    private static PageRangeIndex newIndex(ServerConfiguration conf) throws Exception {
        File tmpDir = File.createTempFile("bkPageRange", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();
        return new PageRangeIndex(conf, KeyValueStorageRocksDB.factory,
                tmpDir.getAbsolutePath(), NullStatsLogger.INSTANCE);
    }

    private static byte[] page(int deptMin, int deptMax, int salMin, int salMax) {
        Map<Short, PageRangeCodec.Range> cols = new HashMap<>();
        cols.put(DEPT, new PageRangeCodec.Range(encInt(deptMin), encInt(deptMax), false, false));
        cols.put(SALARY, new PageRangeCodec.Range(encInt(salMin), encInt(salMax), false, false));
        return PageRangeCodec.encodePage(cols);
    }

    @Test
    public void prunesByDepartmentRange() throws Exception {
        PageRangeIndex idx = newIndex(conf);
        try {
            long ledgerId = 1;
            // entries 0..4 -> departmentId in [1,10]; entries 5..9 -> departmentId in [11,20]
            for (long e = 0; e < 10; e++) {
                int min = e < 5 ? 1 : 11;
                int max = e < 5 ? 10 : 20;
                idx.addPageRanges(ledgerId, e, page(min, max, 10, 300));
            }

            // predicate: departmentId > 10
            Map<Short, List<PageRangeCodec.Range>> pred = new HashMap<>();
            pred.put(DEPT, Collections.singletonList(
                    new PageRangeCodec.Range(encInt(10), null, true, false)));
            byte[] predicate = PageRangeCodec.encode(pred);

            assertEquals(Arrays.asList(5L, 6L, 7L, 8L, 9L),
                    idx.giveIndexPages(ledgerId, 0, 9, predicate));
            // entry-range restriction is honored
            assertEquals(Collections.emptyList(), idx.giveIndexPages(ledgerId, 0, 4, predicate));
            assertEquals(Arrays.asList(6L, 7L, 8L), idx.giveIndexPages(ledgerId, 6, 8, predicate));
        } finally {
            idx.close();
        }
    }

    @Test
    public void prunesByCompoundPredicate() throws Exception {
        PageRangeIndex idx = newIndex(conf);
        try {
            long ledgerId = 7;
            // entry 0: dept[1,5]   salary[10,50]
            // entry 1: dept[6,10]  salary[60,100]
            // entry 2: dept[6,10]  salary[110,200]
            // entry 3: dept[11,20] salary[150,300]
            // entry 4: dept[1,5]   salary[200,300]
            idx.addPageRanges(ledgerId, 0, page(1, 5, 10, 50));
            idx.addPageRanges(ledgerId, 1, page(6, 10, 60, 100));
            idx.addPageRanges(ledgerId, 2, page(6, 10, 110, 200));
            idx.addPageRanges(ledgerId, 3, page(11, 20, 150, 300));
            idx.addPageRanges(ledgerId, 4, page(1, 5, 200, 300));

            // predicate: departmentId > 5 AND salary > 100
            Map<Short, List<PageRangeCodec.Range>> pred = new HashMap<>();
            pred.put(DEPT, Collections.singletonList(
                    new PageRangeCodec.Range(encInt(5), null, true, false)));
            pred.put(SALARY, Collections.singletonList(
                    new PageRangeCodec.Range(encInt(100), null, true, false)));
            byte[] predicate = PageRangeCodec.encode(pred);

            // entry 0 pruned (dept<=5), entry 1 pruned (salary<=100),
            // entries 2,3 kept, entry 4 pruned (dept<=5)
            assertEquals(Arrays.asList(2L, 3L), idx.giveIndexPages(ledgerId, 0, 4, predicate));
        } finally {
            idx.close();
        }
    }

    @Test
    public void recoversFromSidecarRebuild() throws Exception {
        // Re-adding the same range blobs after a wipe (simulating index rebuild from the
        // durable per-entry range sidecars) reproduces identical prune results.
        PageRangeIndex idx = newIndex(conf);
        try {
            long ledgerId = 9;
            List<byte[]> blobs = new ArrayList<>();
            for (long e = 0; e < 6; e++) {
                byte[] blob = page(e < 3 ? 1 : 11, e < 3 ? 10 : 20, 10, 300);
                blobs.add(blob);
                idx.addPageRanges(ledgerId, e, blob);
            }
            Map<Short, List<PageRangeCodec.Range>> pred = new HashMap<>();
            pred.put(DEPT, Collections.singletonList(
                    new PageRangeCodec.Range(encInt(10), null, true, false)));
            byte[] predicate = PageRangeCodec.encode(pred);

            List<Long> before = idx.giveIndexPages(ledgerId, 0, 5, predicate);

            // wipe + rebuild from the same per-entry blobs
            idx.delete(ledgerId);
            assertEquals(Collections.emptyList(), idx.giveIndexPages(ledgerId, 0, 5, predicate));
            for (long e = 0; e < 6; e++) {
                idx.addPageRanges(ledgerId, e, blobs.get((int) e));
            }

            assertEquals(before, idx.giveIndexPages(ledgerId, 0, 5, predicate));
            assertEquals(Arrays.asList(3L, 4L, 5L), before);
        } finally {
            idx.close();
        }
    }
}
