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
package org.apache.pulsar.client.streaminglake;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Bounded top-K for {@code ORDER BY col [ASC|DESC] LIMIT k}: keeps only the K best rows in a heap of
 * size K as rows stream in (O(N log K) time, O(K) memory), instead of sorting the whole scan output.
 * Paired with segment/page zone-map ordering on the read side, most pages can be skipped once the heap
 * is full and a page's extreme can't beat the current Kth row.
 */
public final class StreamLakeTopK {

    private final int k;
    private final int sortColumn;
    private final boolean descending;
    // Comparator ordering the heap so its head is the "worst" kept row (evicted first).
    private final Comparator<Object[]> heapOrder;
    private final PriorityQueue<Object[]> heap;

    public StreamLakeTopK(int k, int sortColumn, boolean descending) {
        this.k = Math.max(1, k);
        this.sortColumn = sortColumn;
        this.descending = descending;
        // For ORDER BY DESC we keep the K largest, so the head must be the smallest kept (natural asc);
        // for ASC we keep the K smallest, so the head must be the largest kept (reverse).
        Comparator<Object[]> asc = Comparator.comparing(r -> asComparable(r[sortColumn]), nullsFirst());
        this.heapOrder = descending ? asc : asc.reversed();
        this.heap = new PriorityQueue<>(this.k, heapOrder);
    }

    /** Offer a row; retained only if it is among the K best seen so far. */
    public void offer(Object[] row) {
        if (heap.size() < k) {
            heap.add(row);
            return;
        }
        // heap.peek() is the worst kept row; replace it if the new row is better.
        if (heapOrder.compare(row, heap.peek()) > 0) {
            heap.poll();
            heap.add(row);
        }
    }

    public void offerAll(Iterable<Object[]> rows) {
        for (Object[] r : rows) {
            offer(r);
        }
    }

    /**
     * Whether a page whose sort-column extreme is {@code pageExtreme} can be skipped entirely: true
     * once the heap is full and that extreme cannot beat the current Kth-best row. {@code pageExtreme}
     * should be the page's max for DESC (its best possible) or its min for ASC.
     */
    public boolean canSkipPage(Object pageExtreme) {
        if (heap.size() < k) {
            return false;
        }
        Object kth = heap.peek()[sortColumn];
        int cmp = compareValues(pageExtreme, kth);
        return descending ? cmp <= 0 : cmp >= 0;
    }

    /** The K best rows, sorted best-first (matching the ORDER BY direction). */
    public List<Object[]> results() {
        List<Object[]> out = new ArrayList<>(heap);
        out.sort(heapOrder.reversed()); // best-first
        return out;
    }

    private int compareValues(Object a, Object b) {
        return nullsFirst().compare(asComparable(a), asComparable(b));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparable<Object> asComparable(Object v) {
        return (Comparable) v;
    }

    private static Comparator<Comparable<Object>> nullsFirst() {
        return Comparator.nullsFirst(Comparator.naturalOrder());
    }
}
