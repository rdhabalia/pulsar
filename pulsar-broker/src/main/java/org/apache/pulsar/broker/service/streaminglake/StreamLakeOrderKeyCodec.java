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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.client.streaminglake.StreamLakeType;

/**
 * Order-preserving byte encoding of a single column value, so RocksDB's <b>default bytewise</b>
 * comparator sorts encoded keys in the value's natural order (used by {@link StreamLakeExternalSort} —
 * no JNI custom comparator needed). Encoding rules:
 *
 * <ul>
 *   <li>a leading <b>null flag</b> ({@code 0x00} null, {@code 0x01} present) — nulls sort first
 *       ascending (last descending, via reverse iteration);</li>
 *   <li>{@code INT32/INT64} — big-endian with the <b>sign bit flipped</b> so negatives precede
 *       positives;</li>
 *   <li>{@code DOUBLE} — IEEE-754 bits; negative → flip all bits, positive → flip the sign bit;</li>
 *   <li>{@code BOOLEAN} — {@code 0x00 / 0x01};</li>
 *   <li>{@code STRING} — UTF-8 (bytewise order == Unicode code-point order);</li>
 *   <li>{@code BYTES} — raw.</li>
 * </ul>
 *
 * <p>Callers append a unique suffix (e.g. a sequence) after this key so equal sort values keep a stable
 * order and don't collide.
 */
public final class StreamLakeOrderKeyCodec {

    private StreamLakeOrderKeyCodec() {
    }

    public static byte[] encode(Object value, StreamLakeType type) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (value == null) {
            out.write(0x00); // null sorts first (ascending)
            return out.toByteArray();
        }
        out.write(0x01);
        switch (type) {
            case INT32:
                putInt(out, ((Number) value).intValue() ^ 0x80000000);
                break;
            case INT64:
                putLong(out, ((Number) value).longValue() ^ 0x8000000000000000L);
                break;
            case DOUBLE: {
                long bits = Double.doubleToLongBits(((Number) value).doubleValue());
                // negative: flip all bits; positive: flip only the sign bit -> total order.
                bits ^= (bits >> 63) | 0x8000000000000000L;
                putLong(out, bits);
                break;
            }
            case BOOLEAN:
                out.write(((Boolean) value) ? 0x01 : 0x00);
                break;
            case STRING: {
                byte[] b = ((String) value).getBytes(StandardCharsets.UTF_8);
                out.write(b, 0, b.length);
                break;
            }
            case BYTES: {
                byte[] b = (byte[]) value;
                out.write(b, 0, b.length);
                break;
            }
            default:
                throw new IllegalArgumentException("Unsupported ORDER BY column type: " + type);
        }
        return out.toByteArray();
    }

    private static void putInt(ByteArrayOutputStream out, int v) {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private static void putLong(ByteArrayOutputStream out, long v) {
        for (int i = 56; i >= 0; i -= 8) {
            out.write((int) ((v >>> i) & 0xFF));
        }
    }
}
