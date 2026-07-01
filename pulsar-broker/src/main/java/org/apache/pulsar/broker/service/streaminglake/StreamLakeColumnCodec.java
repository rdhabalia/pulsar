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

import java.util.Arrays;

/**
 * Lossless integer-column codecs for {@link StreamLakeBatchPage} column blocks. Each column's
 * values are normalized to a {@code long[]} (INT columns are sign-extended 32-bit, LONG columns are
 * the full 64-bit value) and encoded into a self-contained byte block whose codec id is recorded in
 * the page's column directory. Decoding reproduces exactly the values the raw fixed-stride layout
 * would have returned, so a codec choice is invisible to the scan and the transcoder — it only
 * shrinks the stored/transferred bytes.
 *
 * <p>Codecs (all lossless; the smallest is auto-selected when compression is enabled, with
 * {@link #RAW} always a candidate so a column is never larger than its fixed-stride form):
 * <ul>
 *   <li>{@link #RAW} — fixed-stride big-endian (INT=4B, LONG=8B); byte-identical to the original
 *       uncompressed layout, so the page's fast random-access read path is preserved.</li>
 *   <li>{@link #FOR} — frame of reference: store {@code min} once, bit-pack each value's residual
 *       {@code (v - min)}. Wins on clustered / low-range columns.</li>
 *   <li>{@link #DELTA} — successive differences, zig-zag mapped and bit-packed. Wins on monotonic /
 *       sequential columns whose step size varies.</li>
 *   <li>{@link #DOUBLE_DELTA} — second differences (delta of delta), zig-zag mapped and bit-packed.
 *       Wins on near-constant-stride columns (e.g. fixed-cadence timestamps), which collapse to a
 *       handful of bytes.</li>
 *   <li>{@link #DICT} — page-level dictionary of distinct values plus bit-packed codes. Wins on
 *       low-cardinality columns whose values are spread too wide for {@link #FOR}.</li>
 * </ul>
 *
 * <p>Gorilla is intentionally omitted: it targets floating-point columns, which the page never
 * stores (columns are integer-typed).
 */
final class StreamLakeColumnCodec {

    static final byte RAW = 0;
    static final byte FOR = 1;
    static final byte DELTA = 2;
    static final byte DICT = 3;
    static final byte DOUBLE_DELTA = 4;

    private StreamLakeColumnCodec() {
    }

    /** An encoded column block: the chosen codec id and its bytes. */
    static final class Encoded {
        final byte codecId;
        final byte[] bytes;

        Encoded(byte codecId, byte[] bytes) {
            this.codecId = codecId;
            this.bytes = bytes;
        }
    }

    /**
     * Encode {@code n} normalized values of the given page type. When {@code compress} is false the
     * column is stored {@link #RAW} (fixed stride). When true, every applicable codec is tried and the
     * smallest block is returned (ties prefer the cheaper-to-decode codec).
     */
    static Encoded encode(long[] values, int n, byte type, boolean compress) {
        boolean isLong = type == StreamLakeBatchPage.TYPE_LONG;
        long[] norm = normalize(values, n, isLong);
        byte[] raw = encodeRaw(norm, n, isLong);
        if (!compress) {
            return new Encoded(RAW, raw);
        }
        Encoded best = new Encoded(RAW, raw);
        best = smaller(best, FOR, encodeFor(norm, n));
        best = smaller(best, DELTA, encodeDelta(norm, n));
        best = smaller(best, DOUBLE_DELTA, encodeDoubleDelta(norm, n));
        byte[] dict = encodeDict(norm, n);
        if (dict != null) {
            best = smaller(best, DICT, dict);
        }
        return best;
    }

    /** Decode a column block of {@code n} values into normalized {@code long[]}. */
    static long[] decode(byte codecId, byte[] blob, int n) {
        switch (codecId) {
            case RAW:
                return decodeRaw(blob, n);
            case FOR:
                return decodeFor(blob, n);
            case DELTA:
                return decodeDelta(blob, n);
            case DOUBLE_DELTA:
                return decodeDoubleDelta(blob, n);
            case DICT:
                return decodeDict(blob, n);
            default:
                throw new IllegalArgumentException("unknown column codec: " + codecId);
        }
    }

    private static Encoded smaller(Encoded current, byte codecId, byte[] candidate) {
        return candidate != null && candidate.length < current.bytes.length
                ? new Encoded(codecId, candidate) : current;
    }

    /** Normalize so a decoded value matches the raw fixed-stride read (INT = sign-extended 32-bit). */
    private static long[] normalize(long[] values, int n, boolean isLong) {
        long[] norm = new long[n];
        for (int i = 0; i < n; i++) {
            norm[i] = isLong ? values[i] : (long) (int) values[i];
        }
        return norm;
    }

    // ------------------------------------------------------------------ RAW

    private static byte[] encodeRaw(long[] norm, int n, boolean isLong) {
        int width = isLong ? 8 : 4;
        byte[] out = new byte[n * width];
        int p = 0;
        for (int i = 0; i < n; i++) {
            long v = norm[i];
            if (isLong) {
                for (int s = 56; s >= 0; s -= 8) {
                    out[p++] = (byte) (v >>> s);
                }
            } else {
                int iv = (int) v;
                out[p++] = (byte) (iv >>> 24);
                out[p++] = (byte) (iv >>> 16);
                out[p++] = (byte) (iv >>> 8);
                out[p++] = (byte) iv;
            }
        }
        return out;
    }

    private static long[] decodeRaw(byte[] blob, int n) {
        long[] out = new long[n];
        boolean isLong = blob.length == n * 8L;
        int p = 0;
        for (int i = 0; i < n; i++) {
            if (isLong) {
                long v = 0;
                for (int b = 0; b < 8; b++) {
                    v = (v << 8) | (blob[p++] & 0xFFL);
                }
                out[i] = v;
            } else {
                int v = ((blob[p] & 0xFF) << 24) | ((blob[p + 1] & 0xFF) << 16)
                        | ((blob[p + 2] & 0xFF) << 8) | (blob[p + 3] & 0xFF);
                p += 4;
                out[i] = v; // sign-extends
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ FOR (frame of reference)

    private static byte[] encodeFor(long[] norm, int n) {
        if (n == 0) {
            return null;
        }
        long min = norm[0];
        long maxResidual = 0;
        for (int i = 0; i < n; i++) {
            min = Math.min(min, norm[i]);
        }
        for (int i = 0; i < n; i++) {
            long r = norm[i] - min; // unsigned residual (two's-complement modular)
            if (Long.compareUnsigned(r, maxResidual) > 0) {
                maxResidual = r;
            }
        }
        int bits = bitWidth(maxResidual);
        byte[] packed = packBits(residuals(norm, n, min), n, bits);
        byte[] out = new byte[8 + 1 + packed.length];
        writeLong(out, 0, min);
        out[8] = (byte) bits;
        System.arraycopy(packed, 0, out, 9, packed.length);
        return out;
    }

    private static long[] residuals(long[] norm, int n, long min) {
        long[] r = new long[n];
        for (int i = 0; i < n; i++) {
            r[i] = norm[i] - min;
        }
        return r;
    }

    private static long[] decodeFor(byte[] blob, int n) {
        long min = readLong(blob, 0);
        int bits = blob[8] & 0xFF;
        long[] residuals = new long[n];
        unpackBits(blob, 9, n, bits, residuals);
        long[] out = new long[n];
        for (int i = 0; i < n; i++) {
            out[i] = min + residuals[i];
        }
        return out;
    }

    // ------------------------------------------------------------------ DELTA (zig-zag deltas)

    private static byte[] encodeDelta(long[] norm, int n) {
        if (n == 0) {
            return null;
        }
        long[] zz = new long[n - 1];
        long maxZz = 0;
        long prev = norm[0];
        for (int i = 1; i < n; i++) {
            long z = zigzag(norm[i] - prev);
            zz[i - 1] = z;
            if (Long.compareUnsigned(z, maxZz) > 0) {
                maxZz = z;
            }
            prev = norm[i];
        }
        int bits = bitWidth(maxZz);
        byte[] packed = packBits(zz, n - 1, bits);
        byte[] out = new byte[8 + 1 + packed.length];
        writeLong(out, 0, norm[0]);
        out[8] = (byte) bits;
        System.arraycopy(packed, 0, out, 9, packed.length);
        return out;
    }

    private static long[] decodeDelta(byte[] blob, int n) {
        long[] out = new long[n];
        if (n == 0) {
            return out;
        }
        long first = readLong(blob, 0);
        int bits = blob[8] & 0xFF;
        long[] zz = new long[n - 1];
        unpackBits(blob, 9, n - 1, bits, zz);
        out[0] = first;
        for (int i = 1; i < n; i++) {
            out[i] = out[i - 1] + unzigzag(zz[i - 1]);
        }
        return out;
    }

    // ------------------------------------------------------------------ DOUBLE_DELTA (delta of delta)

    private static byte[] encodeDoubleDelta(long[] norm, int n) {
        if (n < 2) {
            return null;
        }
        long first = norm[0];
        long firstDelta = norm[1] - norm[0];
        int m = n - 2;
        long[] zz = new long[m];
        long maxZz = 0;
        long prevDelta = firstDelta;
        for (int i = 2; i < n; i++) {
            long delta = norm[i] - norm[i - 1];
            long z = zigzag(delta - prevDelta);
            zz[i - 2] = z;
            if (Long.compareUnsigned(z, maxZz) > 0) {
                maxZz = z;
            }
            prevDelta = delta;
        }
        int bits = bitWidth(maxZz);
        byte[] packed = packBits(zz, m, bits);
        byte[] out = new byte[8 + 8 + 1 + packed.length];
        writeLong(out, 0, first);
        writeLong(out, 8, firstDelta);
        out[16] = (byte) bits;
        System.arraycopy(packed, 0, out, 17, packed.length);
        return out;
    }

    private static long[] decodeDoubleDelta(byte[] blob, int n) {
        long[] out = new long[n];
        if (n == 0) {
            return out;
        }
        out[0] = readLong(blob, 0);
        if (n == 1) {
            return out;
        }
        long firstDelta = readLong(blob, 8);
        int bits = blob[16] & 0xFF;
        int m = n - 2;
        long[] zz = new long[m];
        unpackBits(blob, 17, m, bits, zz);
        out[1] = out[0] + firstDelta;
        long prevDelta = firstDelta;
        for (int i = 2; i < n; i++) {
            long delta = prevDelta + unzigzag(zz[i - 2]);
            out[i] = out[i - 1] + delta;
            prevDelta = delta;
        }
        return out;
    }

    // ------------------------------------------------------------------ DICT (page dictionary)

    private static byte[] encodeDict(long[] norm, int n) {
        if (n == 0) {
            return null;
        }
        long[] sorted = Arrays.copyOf(norm, n);
        Arrays.sort(sorted);
        int distinct = 1;
        for (int i = 1; i < n; i++) {
            if (sorted[i] != sorted[i - 1]) {
                distinct++;
            }
        }
        // only worthwhile when there is real repetition; otherwise FOR/RAW win anyway.
        if (distinct > n / 2 || distinct > 65535) {
            return null;
        }
        long[] dict = new long[distinct];
        int d = 0;
        dict[d++] = sorted[0];
        for (int i = 1; i < n; i++) {
            if (sorted[i] != sorted[i - 1]) {
                dict[d++] = sorted[i];
            }
        }
        long[] codes = new long[n];
        for (int i = 0; i < n; i++) {
            codes[i] = lowerBound(dict, norm[i]);
        }
        int bits = bitWidth(distinct - 1);
        byte[] packed = packBits(codes, n, bits);
        byte[] out = new byte[4 + distinct * 8 + 1 + packed.length];
        writeInt(out, 0, distinct);
        int p = 4;
        for (int i = 0; i < distinct; i++) {
            writeLong(out, p, dict[i]);
            p += 8;
        }
        out[p++] = (byte) bits;
        System.arraycopy(packed, 0, out, p, packed.length);
        return out;
    }

    private static long[] decodeDict(byte[] blob, int n) {
        int distinct = readInt(blob, 0);
        long[] dict = new long[distinct];
        int p = 4;
        for (int i = 0; i < distinct; i++) {
            dict[i] = readLong(blob, p);
            p += 8;
        }
        int bits = blob[p++] & 0xFF;
        long[] codes = new long[n];
        unpackBits(blob, p, n, bits, codes);
        long[] out = new long[n];
        for (int i = 0; i < n; i++) {
            out[i] = dict[(int) codes[i]];
        }
        return out;
    }

    /** Index of {@code key} in sorted {@code dict} (always present for DICT codes). */
    private static int lowerBound(long[] dict, long key) {
        int lo = 0;
        int hi = dict.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (dict[mid] < key) {
                lo = mid + 1;
            } else if (dict[mid] > key) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return lo; // unreachable for present keys
    }

    // ------------------------------------------------------------------ bit packing

    /** Bits needed to hold {@code value} treated as unsigned (0 for value 0, up to 64). */
    static int bitWidth(long value) {
        return value == 0 ? 0 : 64 - Long.numberOfLeadingZeros(value);
    }

    /** LSB-first pack of {@code n} values, {@code bits} each (0..64), into a fresh byte array. */
    static byte[] packBits(long[] values, int n, int bits) {
        if (bits == 0 || n == 0) {
            return new byte[0];
        }
        byte[] out = new byte[(int) (((long) n * bits + 7) / 8)];
        long bitPos = 0;
        for (int i = 0; i < n; i++) {
            long v = bits == 64 ? values[i] : (values[i] & ((1L << bits) - 1));
            int remaining = bits;
            int srcBit = 0;
            while (remaining > 0) {
                int byteIdx = (int) (bitPos >>> 3);
                int bitInByte = (int) (bitPos & 7);
                int take = Math.min(8 - bitInByte, remaining);
                int chunk = (int) ((v >>> srcBit) & ((1L << take) - 1));
                out[byteIdx] |= chunk << bitInByte;
                bitPos += take;
                srcBit += take;
                remaining -= take;
            }
        }
        return out;
    }

    /** Inverse of {@link #packBits}: read {@code n} values of {@code bits} each from {@code in[off..]}. */
    static void unpackBits(byte[] in, int off, int n, int bits, long[] out) {
        if (bits == 0) {
            Arrays.fill(out, 0, n, 0L);
            return;
        }
        long bitPos = (long) off * 8;
        for (int i = 0; i < n; i++) {
            long v = 0;
            int got = 0;
            int remaining = bits;
            while (remaining > 0) {
                int byteIdx = (int) (bitPos >>> 3);
                int bitInByte = (int) (bitPos & 7);
                int take = Math.min(8 - bitInByte, remaining);
                int chunk = ((in[byteIdx] & 0xFF) >>> bitInByte) & ((1 << take) - 1);
                v |= ((long) chunk) << got;
                got += take;
                bitPos += take;
                remaining -= take;
            }
            out[i] = v;
        }
    }

    // ------------------------------------------------------------------ small helpers

    private static long zigzag(long v) {
        return (v << 1) ^ (v >> 63);
    }

    private static long unzigzag(long z) {
        return (z >>> 1) ^ -(z & 1);
    }

    private static void writeInt(byte[] b, int off, int v) {
        b[off] = (byte) (v >>> 24);
        b[off + 1] = (byte) (v >>> 16);
        b[off + 2] = (byte) (v >>> 8);
        b[off + 3] = (byte) v;
    }

    private static int readInt(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16)
                | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }

    private static void writeLong(byte[] b, int off, long v) {
        for (int i = 0; i < 8; i++) {
            b[off + i] = (byte) (v >>> (56 - i * 8));
        }
    }

    private static long readLong(byte[] b, int off) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[off + i] & 0xFFL);
        }
        return v;
    }
}
