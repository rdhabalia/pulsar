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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Unit tests for the Streaming Lake bloom-filter / key-set page pruning (no bookie needed).
 */
public class PageRangeBloomTest {

    private static final short COL = 1;

    private static byte[] enc(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    @Test
    public void bloomHasNoFalseNegativesAndLowFalsePositives() {
        List<byte[]> present = new ArrayList<>();
        for (long v = 0; v < 1000; v++) {
            present.add(enc(v));
        }
        byte[] bloom = BloomFilter.build(present, 12);

        // no false negatives — every inserted value must report present
        for (byte[] v : present) {
            assertTrue(BloomFilter.mightContain(bloom, v));
        }
        // false-positive rate on 100k absent values should be small
        int fp = 0;
        for (long v = 1_000_000; v < 1_100_000; v++) {
            if (BloomFilter.mightContain(bloom, enc(v))) {
                fp++;
            }
        }
        assertTrue("false-positive rate too high: " + fp + "/100000", fp < 3000); // < 3%
    }

    @Test
    public void keySetPredicatePrunesPagesByBloom() {
        // page holds values {100,101,102}; build its bloom + a range
        List<byte[]> values = new ArrayList<>();
        for (long v = 100; v <= 102; v++) {
            values.add(enc(v));
        }
        Map<Short, PageRangeCodec.Range> pageRanges = new HashMap<>();
        pageRanges.put(COL, new PageRangeCodec.Range(enc(100), enc(102), false, false));
        Map<Short, byte[]> blooms = new HashMap<>();
        blooms.put(COL, BloomFilter.build(values, 16));
        byte[] pageBlob = PageRangeCodec.encodePage(pageRanges, blooms);

        // predicate probing a key the page contains -> keep
        byte[] hitPred = PageRangeCodec.encodePredicate(new HashMap<>(),
                singleKeySet(COL, enc(101)));
        assertTrue(PageRangeCodec.pageCouldMatch(pageBlob, hitPred));

        // predicate probing keys the page does not contain -> prune
        byte[] missPred = PageRangeCodec.encodePredicate(new HashMap<>(),
                singleKeySet(COL, enc(900_000)));
        assertFalse(PageRangeCodec.pageCouldMatch(pageBlob, missPred));

        // semi-join shape: a key-set spanning {101, 900000} overlaps -> keep
        Map<Short, List<byte[]>> mixed = new HashMap<>();
        mixed.put(COL, java.util.Arrays.asList(enc(101), enc(900_000)));
        assertTrue(PageRangeCodec.pageCouldMatch(pageBlob,
                PageRangeCodec.encodePredicate(new HashMap<>(), mixed)));
    }

    @Test
    public void pageWithoutBloomIsConservativelyKept() {
        // old-style page blob (range only, no bloom)
        Map<Short, PageRangeCodec.Range> pageRanges = new HashMap<>();
        pageRanges.put(COL, new PageRangeCodec.Range(enc(100), enc(102), false, false));
        byte[] pageBlob = PageRangeCodec.encodePage(pageRanges);

        // a key-set predicate cannot prune a page that has no bloom -> kept (no false negatives)
        byte[] pred = PageRangeCodec.encodePredicate(new HashMap<>(), singleKeySet(COL, enc(900_000)));
        assertTrue(PageRangeCodec.pageCouldMatch(pageBlob, pred));
    }

    @Test
    public void rangeAndKeySetAreAnded() {
        List<byte[]> values = new ArrayList<>();
        for (long v = 100; v <= 102; v++) {
            values.add(enc(v));
        }
        Map<Short, PageRangeCodec.Range> pageRanges = new HashMap<>();
        pageRanges.put(COL, new PageRangeCodec.Range(enc(100), enc(102), false, false));
        Map<Short, byte[]> blooms = new HashMap<>();
        blooms.put(COL, BloomFilter.build(values, 16));
        byte[] pageBlob = PageRangeCodec.encodePage(pageRanges, blooms);

        // range matches (overlaps [100,102]) AND key 101 in bloom -> keep
        Map<Short, List<PageRangeCodec.Range>> rng = new HashMap<>();
        rng.put(COL, Collections.singletonList(new PageRangeCodec.Range(enc(90), enc(110), false, false)));
        assertTrue(PageRangeCodec.pageCouldMatch(pageBlob,
                PageRangeCodec.encodePredicate(rng, singleKeySet(COL, enc(101)))));

        // range does not overlap -> prune regardless of bloom
        Map<Short, List<PageRangeCodec.Range>> rng2 = new HashMap<>();
        rng2.put(COL, Collections.singletonList(new PageRangeCodec.Range(enc(200), enc(300), false, false)));
        assertFalse(PageRangeCodec.pageCouldMatch(pageBlob,
                PageRangeCodec.encodePredicate(rng2, singleKeySet(COL, enc(101)))));
    }

    @Test
    public void decodeAllRoundTrip() {
        Map<Short, PageRangeCodec.Range> pageRanges = new HashMap<>();
        pageRanges.put(COL, new PageRangeCodec.Range(enc(1), enc(5), false, false));
        Map<Short, byte[]> blooms = new HashMap<>();
        blooms.put(COL, BloomFilter.build(Collections.singletonList(enc(3)), 12));
        byte[] blob = PageRangeCodec.encodePage(pageRanges, blooms);

        PageRangeCodec.Decoded d = PageRangeCodec.decodeAll(blob);
        assertEquals(1, d.ranges.size());
        assertEquals(1, d.blooms.size());
        assertTrue(d.keySets.isEmpty());
        assertTrue(BloomFilter.mightContain(d.blooms.get(COL), enc(3)));
    }

    private static Map<Short, List<byte[]>> singleKeySet(short col, byte[] key) {
        Map<Short, List<byte[]>> m = new HashMap<>();
        m.put(col, Collections.singletonList(key));
        return m;
    }
}
