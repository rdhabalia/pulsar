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

import java.util.Random;
import org.apache.pulsar.broker.service.streaminglake.StreamLakeColumnCodec.Encoded;
import org.testng.annotations.Test;

/**
 * Pure unit tests (no broker/bookie) for {@link StreamLakeColumnCodec}: every codec round-trips
 * losslessly, auto-selection shrinks compressible columns while never beating raw on random data,
 * and the bit-packer is exact across all widths.
 */
public class StreamLakeColumnCodecTest {

    private static final byte INT = StreamLakeBatchPage.TYPE_INT;
    private static final byte LONG = StreamLakeBatchPage.TYPE_LONG;

    /** Normalized expectation: INT columns are read back sign-extended from 32 bits. */
    private static long[] normalize(long[] in, byte type) {
        long[] out = new long[in.length];
        for (int i = 0; i < in.length; i++) {
            out[i] = type == LONG ? in[i] : (long) (int) in[i];
        }
        return out;
    }

    private static void assertRoundTrips(long[] values, byte type) {
        long[] expected = normalize(values, type);
        // compression on -> auto-selected codec must decode back to the normalized values
        Encoded comp = StreamLakeColumnCodec.encode(values, values.length, type, true);
        assertEquals(StreamLakeColumnCodec.decode(comp.codecId, comp.bytes, values.length), expected);
        // compression off -> RAW, byte-identical fixed stride, also decodes back
        Encoded raw = StreamLakeColumnCodec.encode(values, values.length, type, false);
        assertEquals(raw.codecId, StreamLakeColumnCodec.RAW);
        assertEquals(raw.bytes.length, values.length * (type == LONG ? 8 : 4));
        assertEquals(StreamLakeColumnCodec.decode(raw.codecId, raw.bytes, values.length), expected);
    }

    @Test
    public void roundTripsAcrossPatternsAndTypes() {
        int n = 257; // not a multiple of 8, to exercise bit-pack tails
        long[] constant = new long[n];
        long[] sequential = new long[n];
        long[] lowRange = new long[n];
        long[] lowCardWide = new long[n];
        long[] descending = new long[n];
        long[] randomWide = new long[n];
        long[] dict4 = {5, 1_000_000, -7, 999_999_999L};
        Random r = new Random(42);
        for (int i = 0; i < n; i++) {
            constant[i] = 123_456_789L;
            sequential[i] = 1_700_000_000_000L + i * 1000L;
            lowRange[i] = 500 + (i * 7) % 250;
            lowCardWide[i] = dict4[i % 4];
            descending[i] = 9_000_000_000L - i * 3L;
            randomWide[i] = r.nextLong();
        }
        for (byte type : new byte[]{INT, LONG}) {
            assertRoundTrips(constant, type);
            assertRoundTrips(lowRange, type);
            assertRoundTrips(lowCardWide, type);
            assertRoundTrips(randomWide, type);
        }
        // sequential/descending span beyond 32 bits -> meaningful only as LONG
        assertRoundTrips(sequential, LONG);
        assertRoundTrips(descending, LONG);
    }

    @Test
    public void edgeSizesRoundTrip() {
        assertRoundTrips(new long[]{}, LONG);
        assertRoundTrips(new long[]{}, INT);
        assertRoundTrips(new long[]{42}, LONG);
        assertRoundTrips(new long[]{-42}, INT);
        assertRoundTrips(new long[]{Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, 1}, LONG);
        assertRoundTrips(new long[]{Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1}, INT);
    }

    @Test
    public void intNormalizationTruncatesToThirtyTwoBits() {
        // high bits above bit 31 must be dropped and the result sign-extended, matching the raw read.
        long[] values = {0x1_0000_0005L, 0xFFFF_FFFFL /* -> -1 */, 0x7FFF_FFFFL};
        long[] expected = {5, -1, 0x7FFF_FFFFL};
        Encoded comp = StreamLakeColumnCodec.encode(values, values.length, INT, true);
        assertEquals(StreamLakeColumnCodec.decode(comp.codecId, comp.bytes, values.length), expected);
    }

    @Test
    public void forChosenForLowRange() {
        int n = 300;
        long[] v = new long[n];
        for (int i = 0; i < n; i++) {
            v[i] = 500 + (i * 7) % 250; // narrow range, high distinct count (dict disabled)
        }
        Encoded e = StreamLakeColumnCodec.encode(v, n, LONG, true);
        assertEquals(e.codecId, StreamLakeColumnCodec.FOR, "narrow-range column should pick frame-of-reference");
        assertTrue(e.bytes.length < n * 8, "FOR must be smaller than raw 8B/value");
        assertEquals(StreamLakeColumnCodec.decode(e.codecId, e.bytes, n), v);
    }

    @Test
    public void deltaChosenForRandomGapMonotonic() {
        int n = 300;
        long[] v = new long[n];
        Random r = new Random(123);
        long acc = 1_000_000_000_000L;
        for (int i = 0; i < n; i++) {
            acc += 1 + r.nextInt(15); // strictly increasing, small irregular steps
            v[i] = acc;
        }
        Encoded e = StreamLakeColumnCodec.encode(v, n, LONG, true);
        assertEquals(e.codecId, StreamLakeColumnCodec.DELTA, "irregular-step monotonic column should pick delta");
        assertTrue(e.bytes.length < n * 2, "5-bit zig-zag deltas pack to well under raw 8B/value");
        assertEquals(StreamLakeColumnCodec.decode(e.codecId, e.bytes, n), v);
    }

    @Test
    public void doubleDeltaChosenForConstantStride() {
        int n = 500;
        long[] v = new long[n];
        for (int i = 0; i < n; i++) {
            v[i] = 1_700_000_000_000L + i * 1000L; // fixed-cadence timestamp
        }
        Encoded e = StreamLakeColumnCodec.encode(v, n, LONG, true);
        assertEquals(e.codecId, StreamLakeColumnCodec.DOUBLE_DELTA,
                "constant-stride column should collapse to double-delta");
        assertTrue(e.bytes.length < 64, "constant second-difference is zero bits: a tiny fixed block");
        assertEquals(StreamLakeColumnCodec.decode(e.codecId, e.bytes, n), v);
    }

    @Test
    public void dictChosenForLowCardinalityWideRange() {
        int n = 400;
        long[] dict = {5, 1_000_000, -7, 999_999_999L};
        long[] v = new long[n];
        for (int i = 0; i < n; i++) {
            v[i] = dict[i % 4];
        }
        Encoded e = StreamLakeColumnCodec.encode(v, n, LONG, true);
        assertEquals(e.codecId, StreamLakeColumnCodec.DICT, "few distinct wide values should pick dictionary");
        assertTrue(e.bytes.length < n * 2, "4-entry dict + 2-bit codes is tiny");
        assertEquals(StreamLakeColumnCodec.decode(e.codecId, e.bytes, n), v);
    }

    @Test
    public void rawChosenForIncompressible() {
        int n = 400;
        long[] v = new long[n];
        Random r = new Random(7);
        for (int i = 0; i < n; i++) {
            v[i] = r.nextLong();
        }
        Encoded e = StreamLakeColumnCodec.encode(v, n, LONG, true);
        assertEquals(e.codecId, StreamLakeColumnCodec.RAW, "random 64-bit data should not beat raw");
        assertEquals(e.bytes.length, n * 8);
    }

    @Test
    public void bitPackRoundTripAcrossWidths() {
        Random r = new Random(99);
        int n = 1000;
        for (int bits = 0; bits <= 64; bits++) {
            long mask = bits == 0 ? 0 : (bits == 64 ? -1L : (1L << bits) - 1);
            long[] in = new long[n];
            for (int i = 0; i < n; i++) {
                in[i] = r.nextLong() & mask;
            }
            byte[] packed = StreamLakeColumnCodec.packBits(in, n, bits);
            assertEquals(packed.length, (int) (((long) n * bits + 7) / 8), "packed size for bits=" + bits);
            long[] out = new long[n];
            StreamLakeColumnCodec.unpackBits(packed, 0, n, bits, out);
            assertEquals(out, in, "bit pack/unpack mismatch at bits=" + bits);
        }
    }

    @Test
    public void bitWidthBoundaries() {
        assertEquals(StreamLakeColumnCodec.bitWidth(0), 0);
        assertEquals(StreamLakeColumnCodec.bitWidth(1), 1);
        assertEquals(StreamLakeColumnCodec.bitWidth(255), 8);
        assertEquals(StreamLakeColumnCodec.bitWidth(256), 9);
        assertEquals(StreamLakeColumnCodec.bitWidth(-1L), 64); // unsigned max
    }
}
