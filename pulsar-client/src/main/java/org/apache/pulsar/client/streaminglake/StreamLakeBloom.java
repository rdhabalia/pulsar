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

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * A compact bloom filter for high-cardinality StreamLake columns: membership pruning where a per-batch
 * exact set would be too large. Documented, language-portable format (murmur3_128 hash +
 * Kirsch-Mitzenmacher double hashing): {@code m(int32) k(int32) words(m/64 * int64)}. No false
 * negatives; false-positive probability approximates the configured {@code fpp}.
 *
 * <p>A binary-fuse / XOR filter would be ~30% smaller for the same fpp and is a drop-in size
 * optimization behind this same build/mightContain/encode surface.
 */
final class StreamLakeBloom {

    private final int m;
    private final int k;
    private final long[] words;

    private StreamLakeBloom(int m, int k, long[] words) {
        this.m = m;
        this.k = k;
        this.words = words;
    }

    static StreamLakeBloom build(Collection<byte[]> values, double fpp) {
        int n = Math.max(1, values.size());
        int m = optimalBits(n, fpp);
        int k = optimalHashes(m, n);
        StreamLakeBloom bloom = new StreamLakeBloom(m, k, new long[m >>> 6]);
        for (byte[] v : values) {
            bloom.add(v);
        }
        return bloom;
    }

    void add(byte[] value) {
        long[] h = hash(value);
        for (int i = 0; i < k; i++) {
            int bit = (int) Long.remainderUnsigned(h[0] + (long) i * h[1], m);
            words[bit >>> 6] |= 1L << (bit & 63);
        }
    }

    boolean mightContain(byte[] value) {
        long[] h = hash(value);
        for (int i = 0; i < k; i++) {
            int bit = (int) Long.remainderUnsigned(h[0] + (long) i * h[1], m);
            if ((words[bit >>> 6] & (1L << (bit & 63))) == 0) {
                return false;
            }
        }
        return true;
    }

    byte[] encode() {
        ByteBuffer bb = ByteBuffer.allocate(4 + 4 + words.length * 8);
        bb.putInt(m).putInt(k);
        for (long w : words) {
            bb.putLong(w);
        }
        return bb.array();
    }

    static StreamLakeBloom decode(byte[] blob) {
        ByteBuffer bb = ByteBuffer.wrap(blob);
        int m = bb.getInt();
        int k = bb.getInt();
        long[] words = new long[m >>> 6];
        for (int i = 0; i < words.length; i++) {
            words[i] = bb.getLong();
        }
        return new StreamLakeBloom(m, k, words);
    }

    private static long[] hash(byte[] value) {
        byte[] b = Hashing.murmur3_128().hashBytes(value).asBytes(); // 16 bytes
        ByteBuffer bb = ByteBuffer.wrap(b);
        return new long[]{bb.getLong(), bb.getLong()};
    }

    private static int optimalBits(int n, double fpp) {
        int m = (int) Math.ceil(-n * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        return Math.max(64, (m + 63) & ~63); // multiple of 64, at least one word
    }

    private static int optimalHashes(int m, int n) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }
}
