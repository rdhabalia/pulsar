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

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.bookie.storage.ldb.PageRangeCodec;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Builds the opaque column-range blob the bookie indexes for a StreamLake entry.
 *
 * <p>The broker reads the configured indexed columns from message metadata properties, encodes
 * each value order-preservingly (so the bookie can compare ranges without understanding the
 * schema), and packs them into a page-range blob via {@link PageRangeCodec}. For a single
 * message the min and max of each column are the same value.
 */
public final class StreamLakeRangeBuilder {

    private StreamLakeRangeBuilder() {
    }

    /**
     * @return the page-range blob for this entry, or {@code null} if no indexed column was found
     *     (e.g. not a StreamLake topic, or the message has none of the configured properties).
     */
    public static byte[] build(StreamingLakeConfig config, ByteBuf headersAndPayload) {
        if (config == null || config.getIndexedColumns() == null || config.getIndexedColumns().isEmpty()) {
            return null;
        }
        Map<String, String> props = readProperties(headersAndPayload);
        if (props.isEmpty()) {
            return null;
        }
        Map<Short, PageRangeCodec.Range> ranges = new HashMap<>();
        for (StreamingLakeConfig.IndexedColumn col : config.getIndexedColumns()) {
            String raw = props.get(col.getName());
            if (raw == null) {
                continue;
            }
            byte[] enc = encode(col.getType(), raw);
            if (enc != null) {
                ranges.put((short) col.getColumnId(), new PageRangeCodec.Range(enc, enc, false, false));
            }
        }
        return ranges.isEmpty() ? null : PageRangeCodec.encodePage(ranges);
    }

    /**
     * Aggregate per-column min/max ranges across a whole page of messages (the bookie indexes
     * one range per column for the page). Returns {@code null} if nothing indexable was found.
     */
    public static byte[] buildForBatch(StreamingLakeConfig config, List<ByteBuf> messages) {
        if (config == null || config.getIndexedColumns() == null || config.getIndexedColumns().isEmpty()) {
            return null;
        }
        Map<Short, byte[]> mins = new HashMap<>();
        Map<Short, byte[]> maxs = new HashMap<>();
        for (ByteBuf m : messages) {
            Map<String, String> props = readProperties(m);
            if (props.isEmpty()) {
                continue;
            }
            for (StreamingLakeConfig.IndexedColumn col : config.getIndexedColumns()) {
                String raw = props.get(col.getName());
                if (raw == null) {
                    continue;
                }
                byte[] enc = encode(col.getType(), raw);
                if (enc == null) {
                    continue;
                }
                short key = (short) col.getColumnId();
                if (!mins.containsKey(key) || compareUnsigned(enc, mins.get(key)) < 0) {
                    mins.put(key, enc);
                }
                if (!maxs.containsKey(key) || compareUnsigned(enc, maxs.get(key)) > 0) {
                    maxs.put(key, enc);
                }
            }
        }
        if (mins.isEmpty()) {
            return null;
        }
        Map<Short, PageRangeCodec.Range> ranges = new HashMap<>();
        for (Map.Entry<Short, byte[]> e : mins.entrySet()) {
            ranges.put(e.getKey(), new PageRangeCodec.Range(e.getValue(), maxs.get(e.getKey()), false, false));
        }
        return PageRangeCodec.encodePage(ranges);
    }

    private static int compareUnsigned(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int x = a[i] & 0xFF;
            int y = b[i] & 0xFF;
            if (x != y) {
                return x - y;
            }
        }
        return a.length - b.length;
    }

    private static Map<String, String> readProperties(ByteBuf headersAndPayload) {
        Map<String, String> props = new HashMap<>();
        try {
            MessageMetadata md = Commands.peekMessageMetadata(headersAndPayload, "streamlake-publish", -1);
            if (md != null) {
                for (KeyValue kv : md.getPropertiesList()) {
                    props.put(kv.getKey(), kv.getValue());
                }
            }
        } catch (Throwable t) {
            // Best-effort: a message we can't parse simply isn't indexed.
            return props;
        }
        return props;
    }

    /** Order-preserving big-endian encoding matching the bookie-side comparison. */
    private static byte[] encode(String type, String value) {
        try {
            switch (type == null ? "" : type.toUpperCase()) {
                case "INT": {
                    int u = Integer.parseInt(value.trim()) ^ 0x80000000;
                    return new byte[]{(byte) (u >>> 24), (byte) (u >>> 16), (byte) (u >>> 8), (byte) u};
                }
                case "LONG": {
                    long u = Long.parseLong(value.trim()) ^ 0x8000000000000000L;
                    byte[] b = new byte[8];
                    for (int i = 7; i >= 0; i--) {
                        b[i] = (byte) u;
                        u >>>= 8;
                    }
                    return b;
                }
                case "STRING":
                    return value.getBytes(StandardCharsets.UTF_8);
                default:
                    return null;
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
