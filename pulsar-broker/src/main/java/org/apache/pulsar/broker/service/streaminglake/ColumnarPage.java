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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Column-major (columnar) page codec for StreamLake topics. A page is a batch of rows
 * stored column-by-column, so each column is contiguous (compresses well, enables
 * vectorized scans). Vortex would slot in here behind the same API; this is the
 * JVM-native encoding.
 *
 * <p>Supported column types: {@link #INT}, {@link #LONG}, {@link #STRING}.
 *
 * <pre>
 *   magic(4)='SLC1' version(1) rowCount(4) numCols(2)
 *   per col: nameLen(2) nameBytes type(1)
 *   per col: column block (INT: n*4, LONG: n*8, STRING: per row [len(4) bytes])
 * </pre>
 */
public final class ColumnarPage {

    public static final byte INT = 0;
    public static final byte LONG = 1;
    public static final byte STRING = 2;

    private static final int MAGIC = 0x534C4331; // 'S','L','C','1'
    private static final byte VERSION = 1;

    private ColumnarPage() {
    }

    /** Decoded page: column names, types and the rows (each row is values by column index). */
    public static final class Decoded {
        public final String[] names;
        public final byte[] types;
        public final List<Object[]> rows;

        Decoded(String[] names, byte[] types, List<Object[]> rows) {
            this.names = names;
            this.types = types;
            this.rows = rows;
        }
    }

    public static byte[] encode(String[] names, byte[] types, List<Object[]> rows) {
        int numCols = names.length;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(out, MAGIC);
        out.write(VERSION);
        writeInt(out, rows.size());
        writeShort(out, numCols);
        for (int c = 0; c < numCols; c++) {
            byte[] nb = names[c].getBytes(StandardCharsets.UTF_8);
            writeShort(out, nb.length);
            out.write(nb, 0, nb.length);
            out.write(types[c]);
        }
        // column-major: write each column's values contiguously
        for (int c = 0; c < numCols; c++) {
            for (Object[] row : rows) {
                Object v = row[c];
                switch (types[c]) {
                    case INT:
                        writeInt(out, ((Number) v).intValue());
                        break;
                    case LONG:
                        writeLong(out, ((Number) v).longValue());
                        break;
                    case STRING:
                        byte[] s = ((String) v).getBytes(StandardCharsets.UTF_8);
                        writeInt(out, s.length);
                        out.write(s, 0, s.length);
                        break;
                    default:
                        throw new IllegalStateException("bad type " + types[c]);
                }
            }
        }
        return out.toByteArray();
    }

    public static Decoded decode(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        if (buf.getInt() != MAGIC) {
            throw new IllegalArgumentException("not a StreamLake columnar page");
        }
        buf.get(); // version
        int rowCount = buf.getInt();
        int numCols = buf.getShort();
        String[] names = new String[numCols];
        byte[] types = new byte[numCols];
        for (int c = 0; c < numCols; c++) {
            int nl = buf.getShort();
            byte[] nb = new byte[nl];
            buf.get(nb);
            names[c] = new String(nb, StandardCharsets.UTF_8);
            types[c] = buf.get();
        }
        List<Object[]> rows = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            rows.add(new Object[numCols]);
        }
        for (int c = 0; c < numCols; c++) {
            for (int r = 0; r < rowCount; r++) {
                switch (types[c]) {
                    case INT:
                        rows.get(r)[c] = buf.getInt();
                        break;
                    case LONG:
                        rows.get(r)[c] = buf.getLong();
                        break;
                    case STRING:
                        int len = buf.getInt();
                        byte[] sb = new byte[len];
                        buf.get(sb);
                        rows.get(r)[c] = new String(sb, StandardCharsets.UTF_8);
                        break;
                    default:
                        throw new IllegalStateException("bad type " + types[c]);
                }
            }
        }
        return new Decoded(names, types, rows);
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

    private static void writeLong(ByteArrayOutputStream out, long v) {
        for (int i = 7; i >= 0; i--) {
            out.write((int) ((v >>> (i * 8)) & 0xFF));
        }
    }
}
