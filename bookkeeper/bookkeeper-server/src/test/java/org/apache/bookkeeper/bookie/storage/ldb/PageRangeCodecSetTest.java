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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Round-trip and backward-compatibility tests for the optional per-page exact-set section of
 * {@link PageRangeCodec} (Streaming Lake phase 1b). The set is what segment-index compaction merges
 * into segment-level sets; a page blob without it (older page, or a predicate blob) must still
 * decode with an empty set map.
 */
public class PageRangeCodecSetTest {

    private static final short C1 = 1;
    private static final short C2 = 2;

    private static byte[] b(int... vals) {
        byte[] out = new byte[vals.length];
        for (int i = 0; i < vals.length; i++) {
            out[i] = (byte) vals[i];
        }
        return out;
    }

    private static Map<Short, PageRangeCodec.Range> ranges() {
        Map<Short, PageRangeCodec.Range> r = new HashMap<>();
        r.put(C1, new PageRangeCodec.Range(b(1), b(9), false, false));
        r.put(C2, new PageRangeCodec.Range(b(10), b(20), false, false));
        return r;
    }

    private static Map<Short, byte[]> blooms() {
        Map<Short, byte[]> bl = new HashMap<>();
        bl.put(C1, b(0xAA, 0xBB));
        return bl;
    }

    @Test
    public void roundTripsPerColumnSets() {
        Map<Short, List<byte[]>> sets = new HashMap<>();
        sets.put(C1, new ArrayList<>(Arrays.asList(b(1), b(5), b(9))));
        // C2 intentionally has no set (e.g. it was high-cardinality) -> min/max only

        PageRangeCodec.Decoded d =
                PageRangeCodec.decodeAll(PageRangeCodec.encodePage(ranges(), blooms(), sets));

        assertEquals(d.sets.size(), 1);
        List<byte[]> c1 = d.sets.get(C1);
        assertEquals(c1.size(), 3);
        assertArrayEquals(b(1), c1.get(0));
        assertArrayEquals(b(5), c1.get(1));
        assertArrayEquals(b(9), c1.get(2));
        assertTrue("C2 has no exact set", d.sets.get(C2) == null);

        // ranges + bloom still intact alongside the new set section
        assertArrayEquals(b(1), d.ranges.get(C1).get(0).min);
        assertArrayEquals(b(9), d.ranges.get(C1).get(0).max);
        assertArrayEquals(b(0xAA, 0xBB), d.blooms.get(C1));
    }

    @Test
    public void twoArgPageBlobHasNoSetSection() {
        // encodePage(ranges, blooms) must be byte-compatible with before: no set section.
        PageRangeCodec.Decoded d =
                PageRangeCodec.decodeAll(PageRangeCodec.encodePage(ranges(), blooms()));
        assertTrue(d.sets.isEmpty());
        assertArrayEquals(b(0xAA, 0xBB), d.blooms.get(C1));
    }

    @Test
    public void plainRangeBlobDecodesWithEmptySets() {
        // a bare range blob (no EXT section at all) still decodes.
        PageRangeCodec.Decoded d = PageRangeCodec.decodeAll(PageRangeCodec.encodePage(ranges()));
        assertTrue(d.sets.isEmpty());
        assertTrue(d.blooms.isEmpty());
        assertArrayEquals(b(10), d.ranges.get(C2).get(0).min);
    }

    @Test
    public void predicateBlobHasNoSets() {
        Map<Short, List<PageRangeCodec.Range>> pred = new HashMap<>();
        pred.put(C1, Collections.singletonList(new PageRangeCodec.Range(b(3), null, true, false)));
        Map<Short, List<byte[]>> keySets = new HashMap<>();
        keySets.put(C1, new ArrayList<>(Arrays.asList(b(7))));
        PageRangeCodec.Decoded d =
                PageRangeCodec.decodeAll(PageRangeCodec.encodePredicate(pred, keySets));
        assertTrue(d.sets.isEmpty());
        assertEquals(d.keySets.get(C1).size(), 1);
    }

    @Test
    public void emptySetMapWritesAnEmptySection() {
        PageRangeCodec.Decoded d = PageRangeCodec.decodeAll(
                PageRangeCodec.encodePage(ranges(), blooms(), new HashMap<>()));
        assertTrue(d.sets.isEmpty());
    }
}
