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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.testng.annotations.Test;

/**
 * Verifies the Streaming Lake contracts that do NOT need a running cluster:
 * the broker's predicate encoder must be byte-compatible with the bookie's decoder,
 * and the date index must prune correctly. The full produce->scan end-to-end requires
 * a MiniCluster and is documented as a (disabled) skeleton below.
 */
public class StreamingLakeContractTest {

    /**
     * The critical cross-codebase contract: bytes the broker emits in a PAGE_PRUNE
     * predicate must decode 1:1 in the bookie's {@link PageRangeCodec}.
     */
    @Test
    public void predicateEncoderMatchesBookieDecoder() {
        // departmentId(col 2) > 1  AND  ( salary(col 3) < 50  OR  salary(col 3) > 100 )
        byte[] blob = new StreamingLakePredicate()
                .greaterThan(2, StreamingLakePredicate.encodeInt(1))
                .lessThan(3, StreamingLakePredicate.encodeInt(50))
                .greaterThan(3, StreamingLakePredicate.encodeInt(100))
                .encode();

        Map<Short, List<PageRangeCodec.Range>> decoded = PageRangeCodec.decode(blob);

        assertEquals(decoded.size(), 2, "two columns referenced");

        List<PageRangeCodec.Range> dept = decoded.get((short) 2);
        assertEquals(dept.size(), 1);
        assertTrue(dept.get(0).minExclusive, "deptId > 1 is an exclusive lower bound");
        assertNull(dept.get(0).max, "deptId > 1 has no upper bound");
        assertEquals(dept.get(0).min, StreamingLakePredicate.encodeInt(1));

        List<PageRangeCodec.Range> salary = decoded.get((short) 3);
        assertEquals(salary.size(), 2, "salary OR produces two candidate ranges");
        // < 50 : (-inf, 50) exclusive upper
        assertNull(salary.get(0).min);
        assertTrue(salary.get(0).maxExclusive);
        // > 100 : (100, +inf) exclusive lower
        assertTrue(salary.get(1).minExclusive);
        assertNull(salary.get(1).max);
    }

    @Test
    public void dateIndexPrunesByDateRange() {
        final long day = 86_400_000L;
        final long d0 = 1_700_000_000_000L; // 2023-11-14T22:13:20Z

        DateIndexLedger idx = new DateIndexLedger();
        idx.append(new DateIndexLedger.LedgerInfo(101, d0, d0 + 1000));
        idx.append(new DateIndexLedger.LedgerInfo(102, d0 + day, d0 + day + 1000));
        idx.append(new DateIndexLedger.LedgerInfo(103, d0 + 2 * day, d0 + 2 * day + 1000));

        assertEquals(idx.partitionCount(), 3);

        // scan the first two days only -> ledgers 101 and 102, never 103
        List<Long> candidates = idx.candidateLedgers(d0, d0 + day + 500);
        assertTrue(candidates.contains(101L));
        assertTrue(candidates.contains(102L));
        assertFalse(candidates.contains(103L), "day-3 ledger pruned by date");
    }

    /**
     * Full produce -> seal columnar pages -> date index -> bookie PAGE_PRUNE -> page read
     * -> columnar decode + row filter -> streamed scan results. Requires a running
     * Pulsar + BookKeeper MiniCluster plus the broker scan-execution and publish-path
     * wiring (handleScan body, handleSend columnar encode, ledgerClosed -> DateIndexLedger).
     * Enabled once that data path is in place.
     */
    @Test(enabled = false)
    public void endToEndScanOverMiniCluster() {
        // 1. start MiniCluster (PulsarTestContext + bookies)
        // 2. admin.topics().createStreamingLakeTopic("persons", indexed: departmentId, salary)
        // 3. produce records across 3 days -> multiple ledgers, multiple sealed pages
        // 4. StreamingLakeConsumer.scan(day0..day1, deptId > 1 AND (salary < 50 OR salary > 100))
        // 5. assert results == brute-force oracle, and metrics show date + range pruning
        //    (mirrors streamLake/StreamingLakeEndToEndTest, but over the live cluster)
    }
}
