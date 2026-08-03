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

import static org.testng.Assert.assertTrue;
import org.apache.pulsar.client.streaminglake.StreamLakeType;
import org.testng.annotations.Test;

/**
 * The order-preserving property that lets RocksDB's default bytewise comparator sort values correctly:
 * for any {@code a < b}, {@code encode(a)} must be bytewise-less-than {@code encode(b)} — including
 * negatives, doubles, strings, and nulls (which sort first).
 */
public class StreamLakeOrderKeyCodecTest {

    private static int cmp(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int d = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (d != 0) {
                return d;
            }
        }
        return a.length - b.length;
    }

    private static void assertAscending(StreamLakeType type, Object... values) {
        for (int i = 0; i + 1 < values.length; i++) {
            byte[] a = StreamLakeOrderKeyCodec.encode(values[i], type);
            byte[] b = StreamLakeOrderKeyCodec.encode(values[i + 1], type);
            assertTrue(cmp(a, b) < 0, "encode(" + values[i] + ") must sort before encode(" + values[i + 1]
                    + ") for " + type);
        }
    }

    @Test
    public void int32Order() {
        assertAscending(StreamLakeType.INT32, null, Integer.MIN_VALUE, -1000, -1, 0, 1, 1000,
                Integer.MAX_VALUE);
    }

    @Test
    public void int64Order() {
        assertAscending(StreamLakeType.INT64, null, Long.MIN_VALUE, -1_000_000L, -1L, 0L, 1L,
                1_000_000L, Long.MAX_VALUE);
    }

    @Test
    public void doubleOrder() {
        assertAscending(StreamLakeType.DOUBLE, null, Double.NEGATIVE_INFINITY, -1.5, -0.0001, 0.0,
                0.0001, 1.5, 1e300, Double.POSITIVE_INFINITY);
    }

    @Test
    public void stringOrder() {
        assertAscending(StreamLakeType.STRING, null, "", "a", "ab", "abc", "b", "z");
    }

    @Test
    public void booleanOrder() {
        assertAscending(StreamLakeType.BOOLEAN, null, false, true);
    }
}
