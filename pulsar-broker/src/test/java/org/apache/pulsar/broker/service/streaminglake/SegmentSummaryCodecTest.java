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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.testng.annotations.Test;

/**
 * Pure unit tests (no broker/bookie) for {@link SegmentSummary} + {@link SegmentSummaryCodec}: the
 * streaming merge algebra (min/max envelope, exact-set union with the cap and "any page missing a
 * set drops the segment set" rules), blob round-trip, and {@code segmentCouldMatch} — including the
 * exact-set sharpening of equality/range/semi-join gates and the conservative min/max fallback.
 */
public class SegmentSummaryCodecTest {

    private static final short C1 = 1;
    private static final short C2 = 2;

    // ---------------------------------------------------------------- helpers

    /** Order-preserving int encoding — the same one the scan and pages use. */
    private static byte[] k(int v) {
        return StreamLakePageScan.encodeKey(v);
    }

    private static int unk(byte[] b) {
        int u = ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
        return u ^ 0x80000000;
    }

    private static PageRangeCodec.Range point(int v) {
        return new PageRangeCodec.Range(k(v), k(v), false, false);
    }

    /** Range with exclusive bounds, matching how the scan encodes {@code gt}/{@code lt}. */
    private static PageRangeCodec.Range range(Integer gt, Integer lt) {
        return new PageRangeCodec.Range(gt == null ? null : k(gt), lt == null ? null : k(lt),
                gt != null, lt != null);
    }

    private static Map<Short, PageRangeCodec.Range> pageRange(short col, int min, int max) {
        Map<Short, PageRangeCodec.Range> m = new HashMap<>();
        m.put(col, new PageRangeCodec.Range(k(min), k(max), false, false));
        return m;
    }

    private static byte[][] set(int... vals) {
        byte[][] out = new byte[vals.length][];
        for (int i = 0; i < vals.length; i++) {
            out[i] = k(vals[i]);
        }
        return out;
    }

    private static Map<Short, byte[][]> pageSet(short col, int... vals) {
        Map<Short, byte[][]> m = new HashMap<>();
        m.put(col, set(vals));
        return m;
    }

    private static byte[] predEq(short col, int v) {
        Map<Short, List<PageRangeCodec.Range>> r = new HashMap<>();
        r.put(col, new ArrayList<>(Arrays.asList(point(v))));
        return PageRangeCodec.encodePredicate(r, new HashMap<>());
    }

    private static byte[] predRange(short col, Integer gt, Integer lt) {
        Map<Short, List<PageRangeCodec.Range>> r = new HashMap<>();
        r.put(col, new ArrayList<>(Arrays.asList(range(gt, lt))));
        return PageRangeCodec.encodePredicate(r, new HashMap<>());
    }

    private static byte[] predKeys(short col, int... vals) {
        Map<Short, List<byte[]>> ks = new HashMap<>();
        List<byte[]> keys = new ArrayList<>();
        for (int v : vals) {
            keys.add(k(v));
        }
        ks.put(col, keys);
        return PageRangeCodec.encodePredicate(new HashMap<>(), ks);
    }

    // ---------------------------------------------------------------- merge algebra

    @Test
    public void mergesMinMaxEnvelopeAcrossPages() {
        SegmentSummary s = new SegmentSummary.Builder(7L, 64)
                .addPage(0, 10, pageRange(C1, 10, 20), null)
                .addPage(1, 10, pageRange(C1, 5, 15), null)
                .addPage(2, 10, pageRange(C1, 30, 40), null)
                .build();
        assertEquals(s.coversLedgerId(), 7L);
        assertEquals(s.numPages(), 3);
        assertEquals(s.numRows(), 30L);
        assertEquals(s.startEntryId(), 0L);
        assertEquals(s.endEntryId(), 2L);
        SegmentSummary.ColumnSummary c = s.column(C1);
        assertEquals(unk(c.min()), 5);
        assertEquals(unk(c.max()), 40);
        assertNull(c.set(), "no per-page sets supplied -> no exact segment set");
    }

    @Test
    public void unionsExactSetsUnderCap() {
        SegmentSummary s = new SegmentSummary.Builder(1L, 4)
                .addPage(0, 5, pageRange(C1, 1, 2), pageSet(C1, 1, 2))
                .addPage(1, 5, pageRange(C1, 2, 3), pageSet(C1, 2, 3))
                .build();
        SegmentSummary.ColumnSummary c = s.column(C1);
        assertEquals(c.set().length, 3, "union {1,2,3}");
        assertTrue(c.setContains(k(1)));
        assertTrue(c.setContains(k(2)));
        assertTrue(c.setContains(k(3)));
        assertFalse(c.setContains(k(4)));
    }

    @Test
    public void dropsExactSetWhenUnionExceedsCap() {
        SegmentSummary s = new SegmentSummary.Builder(1L, 2)
                .addPage(0, 5, pageRange(C1, 1, 2), pageSet(C1, 1, 2))
                .addPage(1, 5, pageRange(C1, 3, 4), pageSet(C1, 3, 4)) // union size 4 > cap 2
                .build();
        assertNull(s.column(C1).set(), "union exceeded cap -> exact set dropped");
        assertEquals(unk(s.column(C1).min()), 1, "min/max still merged");
        assertEquals(unk(s.column(C1).max()), 4);
    }

    @Test
    public void dropsExactSetWhenAnyPageLacksSet() {
        // page 0 has an exact set, page 1 has values (a range) but no set -> segment set can't be exact.
        SegmentSummary s = new SegmentSummary.Builder(1L, 64)
                .addPage(0, 5, pageRange(C1, 1, 2), pageSet(C1, 1, 2))
                .addPage(1, 5, pageRange(C1, 3, 9), null)
                .build();
        assertNull(s.column(C1).set(), "a page carried values but no exact set -> segment set dropped");
        assertEquals(unk(s.column(C1).min()), 1);
        assertEquals(unk(s.column(C1).max()), 9);
    }

    @Test
    public void buildsFromRawL0Blob() {
        byte[] blobA = PageRangeCodec.encodePage(pageRange(C1, 10, 20), new HashMap<>());
        byte[] blobB = PageRangeCodec.encodePage(pageRange(C1, 5, 30), new HashMap<>());
        SegmentSummary s = new SegmentSummary.Builder(3L, 64)
                .addPage(0, 0, blobA)
                .addPage(1, 0, blobB)
                .build();
        assertEquals(unk(s.column(C1).min()), 5);
        assertEquals(unk(s.column(C1).max()), 30);
        assertNull(s.column(C1).set(), "L0 blobs carry no per-page set yet -> segment set null");
    }

    // ---------------------------------------------------------------- round-trip

    @Test
    public void encodeDecodeRoundTrip() {
        SegmentSummary s = new SegmentSummary.Builder(42L, 64)
                .addPage(3, 100, pageRange(C1, 5, 500), pageSet(C1, 5, 100, 500))
                .addPage(4, 100, pageRange(C2, -7, 7), null) // C2 has no exact set
                .build();
        SegmentSummary d = SegmentSummaryCodec.decode(SegmentSummaryCodec.encode(s));

        assertEquals(d.coversLedgerId(), 42L);
        assertEquals(d.startEntryId(), 3L);
        assertEquals(d.endEntryId(), 4L);
        assertEquals(d.numPages(), 2);
        assertEquals(d.numRows(), 200L);
        assertEquals(unk(d.column(C1).min()), 5);
        assertEquals(unk(d.column(C1).max()), 500);
        assertEquals(d.column(C1).set().length, 3);
        assertTrue(d.column(C1).setContains(k(100)));
        assertFalse(d.column(C1).setContains(k(101)));
        assertEquals(unk(d.column(C2).min()), -7);
        assertEquals(unk(d.column(C2).max()), 7);
        assertNull(d.column(C2).set());
    }

    // ---------------------------------------------------------------- segmentCouldMatch: min/max only

    @Test
    public void rangeGateUsesMinMaxWhenNoSet() {
        SegmentSummary s = new SegmentSummary.Builder(1L, 64)
                .addPage(0, 0, pageRange(C1, 10, 40), null)
                .build();
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, predEq(C1, 25)), "25 in [10,40] -> keep");
        assertFalse(SegmentSummaryCodec.segmentCouldMatch(s, predEq(C1, 5)), "5 < 10 -> skip");
        assertFalse(SegmentSummaryCodec.segmentCouldMatch(s, predEq(C1, 50)), "50 > 40 -> skip");
        assertFalse(SegmentSummaryCodec.segmentCouldMatch(s, predRange(C1, 45, null)), "> 45 -> skip");
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, predRange(C1, 5, 50)), "(5,50) overlaps -> keep");
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, predKeys(C1, 25)),
                "no set + no bloom -> key-set gate conservatively keeps");
    }

    // ---------------------------------------------------------------- segmentCouldMatch: exact set

    @Test
    public void exactSetSharpensEqualityAndRange() {
        // non-clustered low-card: values {2,50,98} spread across a wide [0,100] envelope.
        SegmentSummary s = new SegmentSummary.Builder(1L, 64)
                .addPage(0, 0, pageRange(C1, 0, 100), pageSet(C1, 2, 50, 98))
                .build();
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, predEq(C1, 50)), "50 in set");
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, predEq(C1, 2)), "2 in set");
        assertFalse(SegmentSummaryCodec.segmentCouldMatch(s, predEq(C1, 51)),
                "51 inside [0,100] but NOT in set -> skip (the win over min/max)");
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, predRange(C1, 60, 99)), "98 in (60,99) -> keep");
        assertFalse(SegmentSummaryCodec.segmentCouldMatch(s, predRange(C1, 2, 50)),
                "open (2,50) contains no set value -> skip (exact interval pruning)");
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, predRange(C1, 1, 50)), "2 in (1,50) -> keep");
    }

    @Test
    public void keySetGateUsesExactSet() {
        SegmentSummary s = new SegmentSummary.Builder(1L, 64)
                .addPage(0, 0, pageRange(C1, 10, 30), pageSet(C1, 10, 20, 30))
                .build();
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, predKeys(C1, 20)), "20 in set");
        assertFalse(SegmentSummaryCodec.segmentCouldMatch(s, predKeys(C1, 99)), "99 not in set -> skip");
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, predKeys(C1, 5, 99, 20)), "20 present -> keep");
    }

    @Test
    public void joinPushdownAppliesBothGates() {
        // semi-join push-down sends BOTH a runtime range [minKey,maxKey] AND the key-set on the column.
        SegmentSummary s = new SegmentSummary.Builder(1L, 64)
                .addPage(0, 0, pageRange(C1, 10, 30), pageSet(C1, 10, 20, 30))
                .build();
        Map<Short, List<PageRangeCodec.Range>> ranges = new HashMap<>();
        ranges.put(C1, new ArrayList<>(Arrays.asList(range(9, 31))));
        Map<Short, List<byte[]>> keys = new HashMap<>();
        keys.put(C1, new ArrayList<>(Arrays.asList(k(99))));
        byte[] rangeHitKeyMiss = PageRangeCodec.encodePredicate(ranges, keys);
        assertFalse(SegmentSummaryCodec.segmentCouldMatch(s, rangeHitKeyMiss),
                "range gate passes but key 99 not in set -> both gates AND -> skip");

        keys.put(C1, new ArrayList<>(Arrays.asList(k(20))));
        byte[] bothHit = PageRangeCodec.encodePredicate(ranges, keys);
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, bothHit), "range hit AND key 20 in set -> keep");
    }

    // ---------------------------------------------------------------- exactness / safety

    @Test
    public void exactSetGivesExactEqualityPruning() {
        int[] vals = {3, 17, 42, 88, 91};
        Map<Short, byte[][]> ps = pageSet(C1, vals);
        SegmentSummary s = new SegmentSummary.Builder(1L, 64)
                .addPage(0, 0, pageRange(C1, 3, 91), ps)
                .build();
        java.util.Set<Integer> present = new java.util.HashSet<>();
        for (int v : vals) {
            present.add(v);
        }
        for (int x = 0; x <= 100; x++) {
            boolean match = SegmentSummaryCodec.segmentCouldMatch(s, predEq(C1, x));
            assertEquals(match, present.contains(x),
                    "equality on x=" + x + " must be exact against the segment set");
        }
    }

    @Test
    public void unrelatedPredicateColumnKeepsSegment() {
        SegmentSummary s = new SegmentSummary.Builder(1L, 64)
                .addPage(0, 0, pageRange(C1, 10, 20), pageSet(C1, 10, 20))
                .build();
        // predicate only on C2, which the segment has no info about -> conservatively keep.
        assertTrue(SegmentSummaryCodec.segmentCouldMatch(s, predEq(C2, 999)));
    }
}
