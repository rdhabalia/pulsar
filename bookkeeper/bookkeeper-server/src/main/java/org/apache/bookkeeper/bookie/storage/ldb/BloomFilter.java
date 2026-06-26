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

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * A tiny, self-describing, deterministic Bloom filter over opaque {@code byte[]} values, used by
 * the Streaming Lake page index so the broker can build a per-page filter at write time and the
 * bookie can test set membership at prune time — both produce identical results because the
 * hashing is fixed and depends only on the value bytes.
 *
 * <p>Blob layout: {@code byte k | int mBits | bytes(mBits/8)}. {@code mBits} is a power of two so
 * an index is just {@code hash & (mBits-1)}. Membership uses Kirsch–Mitzenmacher double hashing.
 */
public final class BloomFilter {

    private BloomFilter() {
    }

    /** Build a filter sized for {@code values.size()} elements at ~{@code bitsPerElement} bits each. */
    public static byte[] build(Collection<byte[]> values, int bitsPerElement) {
        int n = Math.max(1, values.size());
        int bpe = Math.max(4, bitsPerElement);
        int mBits = nextPowerOfTwo(Math.max(64, n * bpe));
        int k = clamp((int) Math.round(bpe * 0.693), 1, 8); // ln(2) ≈ 0.693
        byte[] bits = new byte[mBits / 8];
        for (byte[] v : values) {
            long h1 = hash(v, 0x9E3779B97F4A7C15L);
            long h2 = hash(v, 0xC2B2AE3D27D4EB4FL) | 1L; // odd, so it's coprime-ish with a power of two
            for (int i = 0; i < k; i++) {
                int idx = (int) ((h1 + (long) i * h2) & (mBits - 1));
                bits[idx >>> 3] |= (byte) (1 << (idx & 7));
            }
        }
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + bits.length);
        buf.put((byte) k);
        buf.putInt(mBits);
        buf.put(bits);
        return buf.array();
    }

    /** Possibly-contains test. {@code false} is definite; {@code true} may be a false positive. */
    public static boolean mightContain(byte[] blob, byte[] value) {
        ByteBuffer buf = ByteBuffer.wrap(blob);
        int k = buf.get() & 0xFF;
        int mBits = buf.getInt();
        int bitsOffset = buf.position();
        long h1 = hash(value, 0x9E3779B97F4A7C15L);
        long h2 = hash(value, 0xC2B2AE3D27D4EB4FL) | 1L;
        for (int i = 0; i < k; i++) {
            int idx = (int) ((h1 + (long) i * h2) & (mBits - 1));
            int b = blob[bitsOffset + (idx >>> 3)] & 0xFF;
            if ((b & (1 << (idx & 7))) == 0) {
                return false;
            }
        }
        return true;
    }

    /** FNV-1a 64-bit over the bytes, mixed with a seed — deterministic and dependency-free. */
    private static long hash(byte[] v, long seed) {
        long h = 0xCBF29CE484222325L ^ seed;
        for (byte b : v) {
            h ^= (b & 0xFF);
            h *= 0x100000001B3L;
        }
        // final avalanche
        h ^= (h >>> 33);
        h *= 0xFF51AFD7ED558CCDL;
        h ^= (h >>> 33);
        return h;
    }

    private static int nextPowerOfTwo(int x) {
        int p = 1;
        while (p < x) {
            p <<= 1;
        }
        return p;
    }

    private static int clamp(int v, int lo, int hi) {
        return Math.max(lo, Math.min(hi, v));
    }
}
