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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * Broker-side broadcast (hash) inner join across two StreamLake topics, with runtime semi-join
 * push-down into the probe side (design: dynamic filtering).
 *
 * <p>Algorithm:
 * <ol>
 *   <li>Scan the <b>build</b> side B (its {@code WHERE} pushed to the bookie), hash its rows by the
 *       join key, and collect the join keys' min/max plus the key set.</li>
 *   <li>Inject into the <b>probe</b> side A's scan a runtime range {@code Bound(key in [min,max])}
 *       (prunes pages by the page-index min/max) and a key-set (prunes pages by the per-page value
 *       bloom). A only reads pages whose key range/bloom can overlap B's keys.</li>
 *   <li>Probe each A row against the hash and emit joined rows. False positives from the bloom are
 *       harmless — the hash probe is the exact membership test.</li>
 * </ol>
 *
 * <p>Memory is bounded by the (filtered) build side; the probe side streams page by page. Pick the
 * smaller side as the build side.
 */
public final class StreamLakeJoin {

    private StreamLakeJoin() {
    }

    /** One side of the join: a topic, its join column (id + property name), filters and date window. */
    public static final class Side {
        public final PersistentTopic topic;
        public final short keyColumnId;
        public final String keyProperty;
        public final List<StreamLakePageScan.Bound> filters;
        public final long fromDate;
        public final long toDate;

        public Side(PersistentTopic topic, short keyColumnId, String keyProperty,
                    List<StreamLakePageScan.Bound> filters, long fromDate, long toDate) {
            this.topic = topic;
            this.keyColumnId = keyColumnId;
            this.keyProperty = keyProperty;
            this.filters = filters == null ? Collections.emptyList() : filters;
            this.fromDate = fromDate;
            this.toDate = toDate;
        }
    }

    /** A joined output row (left = probe/A side, right = build/B side). */
    public static final class JoinRow {
        public final Map<String, String> left;
        public final byte[] leftValue;
        public final Map<String, String> right;
        public final byte[] rightValue;

        JoinRow(Map<String, String> left, byte[] leftValue, Map<String, String> right, byte[] rightValue) {
            this.left = left;
            this.leftValue = leftValue;
            this.right = right;
            this.rightValue = rightValue;
        }
    }

    /** Join result plus stats showing how much the runtime semi-join filter pruned the probe side. */
    public static final class JoinResult {
        public final List<JoinRow> rows;
        public final int buildRows;                 // |B after its WHERE|
        public final int probePagesRead;            // A pages actually read (after range+bloom prune)
        public final int probeLedgersScanned;
        public final int probeLedgersPrunedByDate;

        JoinResult(List<JoinRow> rows, int buildRows, int probePagesRead,
                   int probeLedgersScanned, int probeLedgersPrunedByDate) {
            this.rows = rows;
            this.buildRows = buildRows;
            this.probePagesRead = probePagesRead;
            this.probeLedgersScanned = probeLedgersScanned;
            this.probeLedgersPrunedByDate = probeLedgersPrunedByDate;
        }
    }

    /** Inner equi-join on {@code a.key == b.key}. {@code b} is the build (broadcast) side. */
    public static JoinResult innerJoin(Side a, Side b, BookKeeper bk) throws Exception {
        // 1. build side: scan B and hash by join key.
        StreamLakePageScan.RowResult bRes = StreamLakePageScan.scanRows(
                b.topic, bk, b.filters, Collections.emptyMap(), b.fromDate, b.toDate);
        Map<Integer, List<StreamLakePageScan.Row>> hash = new HashMap<>();
        List<byte[]> keys = new ArrayList<>();
        int minKey = Integer.MAX_VALUE;
        int maxKey = Integer.MIN_VALUE;
        for (StreamLakePageScan.Row r : bRes.rows) {
            Integer k = intProp(r, b.keyProperty);
            if (k == null) {
                continue;
            }
            hash.computeIfAbsent(k, x -> new ArrayList<>()).add(r);
            keys.add(StreamLakePageScan.encodeKey(k));
            minKey = Math.min(minKey, k);
            maxKey = Math.max(maxKey, k);
        }
        if (hash.isEmpty()) {
            return new JoinResult(new ArrayList<>(), bRes.rows.size(), 0, 0, 0);
        }

        // 2. runtime semi-join filters on A's join column: range (min/max) + key-set (bloom).
        List<StreamLakePageScan.Bound> aFilters = new ArrayList<>(a.filters);
        Integer gt = minKey > Integer.MIN_VALUE ? minKey - 1 : null;
        Integer lt = maxKey < Integer.MAX_VALUE ? maxKey + 1 : null;
        aFilters.add(new StreamLakePageScan.Bound(a.keyColumnId, a.keyProperty, gt, lt));
        Map<Short, List<byte[]>> keySet = new HashMap<>();
        keySet.put(a.keyColumnId, keys);

        // 3. probe side: scan A with the runtime filters pushed down.
        StreamLakePageScan.RowResult aRes = StreamLakePageScan.scanRows(
                a.topic, bk, aFilters, keySet, a.fromDate, a.toDate);

        // 4. probe + emit (the hash lookup is the exact membership test).
        List<JoinRow> out = new ArrayList<>();
        for (StreamLakePageScan.Row ar : aRes.rows) {
            Integer k = intProp(ar, a.keyProperty);
            if (k == null) {
                continue;
            }
            List<StreamLakePageScan.Row> matches = hash.get(k);
            if (matches != null) {
                for (StreamLakePageScan.Row br : matches) {
                    out.add(new JoinRow(ar.properties, ar.value, br.properties, br.value));
                }
            }
        }
        return new JoinResult(out, bRes.rows.size(), aRes.pagesRead,
                aRes.ledgersScanned, aRes.ledgersPrunedByDate);
    }

    private static Integer intProp(StreamLakePageScan.Row r, String prop) {
        String v = r.properties.get(prop);
        if (v == null) {
            return null;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
