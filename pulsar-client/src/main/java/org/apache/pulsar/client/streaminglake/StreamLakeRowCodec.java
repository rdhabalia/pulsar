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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * A compact, self-describing codec for a decoded StreamLake row ({@code Object[]} of the values the
 * Arrow decoder produces: {@code null | Integer | Long | Double | Boolean | String | byte[]}). Used to
 * spill hash-join build rows off-heap (to a file) and read them back exactly. Each cell is a one-byte
 * type tag followed by its payload, prefixed by the cell count.
 */
public final class StreamLakeRowCodec {

    private static final byte T_NULL = 0;
    private static final byte T_INT = 1;
    private static final byte T_LONG = 2;
    private static final byte T_DOUBLE = 3;
    private static final byte T_BOOL = 4;
    private static final byte T_STRING = 5;
    private static final byte T_BYTES = 6;

    private StreamLakeRowCodec() {
    }

    public static byte[] encode(Object[] row) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(row.length);
            for (Object v : row) {
                if (v == null) {
                    out.writeByte(T_NULL);
                } else if (v instanceof Integer) {
                    out.writeByte(T_INT);
                    out.writeInt((Integer) v);
                } else if (v instanceof Long) {
                    out.writeByte(T_LONG);
                    out.writeLong((Long) v);
                } else if (v instanceof Double) {
                    out.writeByte(T_DOUBLE);
                    out.writeDouble((Double) v);
                } else if (v instanceof Boolean) {
                    out.writeByte(T_BOOL);
                    out.writeBoolean((Boolean) v);
                } else if (v instanceof String) {
                    byte[] b = ((String) v).getBytes(StandardCharsets.UTF_8);
                    out.writeByte(T_STRING);
                    out.writeInt(b.length);
                    out.write(b);
                } else if (v instanceof byte[]) {
                    byte[] b = (byte[]) v;
                    out.writeByte(T_BYTES);
                    out.writeInt(b.length);
                    out.write(b);
                } else {
                    throw new IllegalArgumentException("Unsupported StreamLake cell type: " + v.getClass());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake row encode failed", e);
        }
        return bos.toByteArray();
    }

    public static Object[] decode(byte[] blob) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(blob))) {
            int n = in.readInt();
            Object[] row = new Object[n];
            for (int i = 0; i < n; i++) {
                byte tag = in.readByte();
                switch (tag) {
                    case T_NULL:
                        row[i] = null;
                        break;
                    case T_INT:
                        row[i] = in.readInt();
                        break;
                    case T_LONG:
                        row[i] = in.readLong();
                        break;
                    case T_DOUBLE:
                        row[i] = in.readDouble();
                        break;
                    case T_BOOL:
                        row[i] = in.readBoolean();
                        break;
                    case T_STRING: {
                        byte[] b = new byte[in.readInt()];
                        in.readFully(b);
                        row[i] = new String(b, StandardCharsets.UTF_8);
                        break;
                    }
                    case T_BYTES: {
                        byte[] b = new byte[in.readInt()];
                        in.readFully(b);
                        row[i] = b;
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Unknown StreamLake cell tag: " + tag);
                }
            }
            return row;
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake row decode failed", e);
        }
    }
}
