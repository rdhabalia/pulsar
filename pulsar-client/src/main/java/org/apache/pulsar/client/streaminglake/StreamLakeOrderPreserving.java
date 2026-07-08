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

import java.nio.charset.StandardCharsets;

/**
 * Order-preserving byte encodings for StreamLake indexed values: the unsigned lexicographic order of
 * the encoded bytes matches the natural (typed) order of the values, so the broker/segment can prune
 * by raw byte comparison (schema-blind) without decoding types. Used for per-column min/max and the
 * values of a low-cardinality exact set.
 */
final class StreamLakeOrderPreserving {

    private StreamLakeOrderPreserving() {
    }

    static byte[] encode(StreamLakeType type, Object value) {
        switch (type) {
            case INT32:
                return encInt(((Number) value).intValue());
            case INT64:
                return encLong(((Number) value).longValue());
            case DOUBLE:
                return encDouble(((Number) value).doubleValue());
            case BOOLEAN:
                return new byte[]{(byte) (((Boolean) value) ? 1 : 0)};
            case STRING:
                return ((String) value).getBytes(StandardCharsets.UTF_8);
            case BYTES:
                return ((byte[]) value).clone();
            default:
                throw new IllegalArgumentException("Unsupported StreamLake type: " + type);
        }
    }

    // Flip the sign bit so two's-complement ints sort correctly as unsigned big-endian bytes.
    static byte[] encInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    static byte[] encLong(long v) {
        return bigEndian(v ^ 0x8000000000000000L);
    }

    // IEEE-754 order-preserving: set the sign bit for positives, flip all bits for negatives.
    static byte[] encDouble(double d) {
        long bits = Double.doubleToLongBits(d);
        bits ^= (bits >> 63) | 0x8000000000000000L;
        return bigEndian(bits);
    }

    private static byte[] bigEndian(long u) {
        byte[] b = new byte[8];
        for (int i = 0; i < 8; i++) {
            b[i] = (byte) (u >>> (56 - i * 8));
        }
        return b;
    }
}
