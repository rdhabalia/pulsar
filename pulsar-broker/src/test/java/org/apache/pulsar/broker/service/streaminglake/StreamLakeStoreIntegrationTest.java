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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * End-to-end column-major StreamLake storage on a real BookKeeper bookie
 * ({@link LocalBookkeeperEnsemble}, which runs {@code DbLedgerStorage} with the page index).
 *
 * <p>Covers the full requested pipeline (Vortex aside): mark a topic StreamLake, batch rows
 * into ~pages, store them column-major, ship schema-derived min/max ranges to the bookie in
 * {@code addEntry}; read pages back and decode them into rows for a normal consumer; and run a
 * StreamLake scan that prunes by date partition in the broker and pushes the rest of the
 * predicate (departmentId &gt; X, salary &gt; Z) to the bookie for column-range pruning.
 */
public class StreamLakeStoreIntegrationTest {

    // schema: name(STRING) departmentId(INT) salary(INT) eventTime(LONG)
    private static final String[] COLS = {"name", "departmentId", "salary", "eventTime"};
    private static final byte[] TYPES =
            {ColumnarPage.STRING, ColumnarPage.INT, ColumnarPage.INT, ColumnarPage.LONG};
    private static final int COL_DEPT = 1;
    private static final int COL_SALARY = 2;
    private static final int COL_TIME = 3;

    private static final int RECORDS = 1000;
    private static final int PAGE_ROWS = 100;
    private static final long BASE_TIME = 1_700_000_000_000L;
    private static final long MINUTE = 60_000L;

    private LocalBookkeeperEnsemble ensemble;
    private BookKeeper bk;

    @BeforeClass
    void setup() throws Exception {
        ensemble = new LocalBookkeeperEnsemble(1, 0);
        ensemble.start();
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri("zk://127.0.0.1:" + ensemble.getZookeeperPort() + "/ledgers");
        bk = new BookKeeper(conf);
    }

    @AfterClass(alwaysRun = true)
    void teardown() throws Exception {
        if (bk != null) {
            bk.close();
        }
        if (ensemble != null) {
            ensemble.stop();
        }
    }

    private static Object[] person(int i) {
        // clustered so each 100-row page holds a narrow departmentId band (2k+1, 2k+2)
        int dept = (i / 50) + 1;
        int salary = 30_000 + (i % 100) * 1_000;
        long eventTime = BASE_TIME + (long) i * MINUTE;
        return new Object[]{"person-" + i, dept, salary, eventTime};
    }

    @Test
    public void columnarStoreReadAndPrunedScan() throws Exception {
        StreamLakeStore store = new StreamLakeStore(bk, COLS, TYPES, COL_TIME,
                new short[]{COL_DEPT, COL_SALARY},
                2 * 1024 * 1024, PAGE_ROWS, 60_000L);

        for (int i = 0; i < RECORDS; i++) {
            store.append(person(i));
        }
        store.seal();

        // --- normal consumer path: every page decodes back to the original rows ---
        List<Object[]> all = store.readAll();
        assertEquals(all.size(), RECORDS);
        for (int i = 0; i < RECORDS; i++) {
            Object[] row = all.get(i);
            assertEquals(row[0], "person-" + i);
            assertEquals(row[COL_DEPT], (i / 50) + 1);
            assertEquals(row[COL_SALARY], 30_000 + (i % 100) * 1_000);
            assertEquals(row[COL_TIME], BASE_TIME + (long) i * MINUTE);
        }

        // --- StreamLake scan: date partition in [rec 200, rec 599] AND dept > 10 AND salary > 50000 ---
        long fromTime = BASE_TIME + 200 * MINUTE;
        long toTime = BASE_TIME + 599 * MINUTE;
        List<StreamLakeStore.Bound> bounds = Arrays.asList(
                new StreamLakeStore.Bound(COL_DEPT, 10, null),
                new StreamLakeStore.Bound(COL_SALARY, 50_000, null));

        List<Object[]> scanned = store.scan(fromTime, toTime, bounds);

        // oracle: brute-force the same predicate over all rows
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < RECORDS; i++) {
            Object[] r = person(i);
            long t = (Long) r[COL_TIME];
            int dept = (Integer) r[COL_DEPT];
            int salary = (Integer) r[COL_SALARY];
            if (t >= fromTime && t <= toTime && dept > 10 && salary > 50_000) {
                expected.add((String) r[0]);
            }
        }
        Set<String> got = new HashSet<>();
        for (Object[] r : scanned) {
            got.add((String) r[0]);
        }
        assertEquals(got, expected, "pruned scan must equal brute-force oracle");
        assertTrue(!expected.isEmpty(), "oracle should select some rows");

        // prove the bookie actually pruned pages: it kept fewer than date prune alone left.
        assertTrue(store.lastBookieKeptPages < store.lastDateCandidatePages,
                "bookie PAGE_PRUNE should drop pages: kept " + store.lastBookieKeptPages
                        + " of " + store.lastDateCandidatePages + " date-candidate pages");

        store.close();
    }
}
