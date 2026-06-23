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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the opaque predicate blob the broker sends to a bookie in a PAGE_PRUNE request.
 *
 * <p>The byte layout MUST match what the bookie's {@code PageRangeCodec} decodes, so the
 * two stay in lock-step:
 * <pre>
 *   short numColumns
 *   per column: short columnId, short numRanges
 *     per range: byte flags (bit0 minPresent, bit1 maxPresent, bit2 minExcl, bit3 maxExcl)
 *                [int minLen, minBytes] if minPresent
 *                [int maxLen, maxBytes] if maxPresent
 * </pre>
 * Values are encoded order-preserving so the bookie can compare with pure byte ordering.
 */
public final class StreamingLakePredicate {

    private static final int MIN_PRESENT = 0x1;
    private static final int MAX_PRESENT = 0x2;
    private static final int MIN_EXCLUSIVE = 0x4;
    private static final int MAX_EXCLUSIVE = 0x8;

    private final Map<Integer, List<Range>> columns = new LinkedHashMap<>();

    /** A single candidate range for a column; null bound == infinity. */
    public static final class Range {
        final byte[] min;
        final byte[] max;
        final boolean minExclusive;
        final boolean maxExclusive;

        public Range(byte[] min, byte[] max, boolean minExclusive, boolean maxExclusive) {
            this.min = min;
            this.max = max;
            this.minExclusive = minExclusive;
            this.maxExclusive = maxExclusive;
        }
    }

    public StreamingLakePredicate addRange(int columnId, Range range) {
        columns.computeIfAbsent(columnId, c -> new ArrayList<>()).add(range);
        return this;
    }

    /** Convenience: {@code column > value} (exclusive lower bound). */
    public StreamingLakePredicate greaterThan(int columnId, byte[] value) {
        return addRange(columnId, new Range(value, null, true, false));
    }

    /** Convenience: {@code column < value} (exclusive upper bound). */
    public StreamingLakePredicate lessThan(int columnId, byte[] value) {
        return addRange(columnId, new Range(null, value, false, true));
    }

    /** Convenience: {@code column == value}. */
    public StreamingLakePredicate equalTo(int columnId, byte[] value) {
        return addRange(columnId, new Range(value, value, false, false));
    }

    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeShort(out, columns.size());
        for (Map.Entry<Integer, List<Range>> e : columns.entrySet()) {
            writeShort(out, e.getKey());
            writeShort(out, e.getValue().size());
            for (Range r : e.getValue()) {
                int flags = 0;
                if (r.min != null) {
                    flags |= MIN_PRESENT;
                }
                if (r.max != null) {
                    flags |= MAX_PRESENT;
                }
                if (r.minExclusive) {
                    flags |= MIN_EXCLUSIVE;
                }
                if (r.maxExclusive) {
                    flags |= MAX_EXCLUSIVE;
                }
                out.write(flags & 0xFF);
                if (r.min != null) {
                    writeInt(out, r.min.length);
                    out.write(r.min, 0, r.min.length);
                }
                if (r.max != null) {
                    writeInt(out, r.max.length);
                    out.write(r.max, 0, r.max.length);
                }
            }
        }
        return out.toByteArray();
    }

    // ---- order-preserving value encoders (match the bookie/codec side) ----

    public static byte[] encodeInt(int v) {
        int u = v ^ 0x80000000;
        return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
    }

    public static byte[] encodeLong(long v) {
        long u = v ^ 0x8000000000000000L;
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) {
            b[i] = (byte) u;
            u >>>= 8;
        }
        return b;
    }

    public static byte[] encodeString(String v) {
        return v.getBytes(StandardCharsets.UTF_8);
    }

    private static void writeShort(ByteArrayOutputStream out, int v) {
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private static void writeInt(ByteArrayOutputStream out, int v) {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }
}
