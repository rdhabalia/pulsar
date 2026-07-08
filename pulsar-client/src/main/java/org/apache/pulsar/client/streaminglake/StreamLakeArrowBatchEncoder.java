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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.util.Text;

/**
 * Encodes StreamLake rows into a self-describing Apache Arrow IPC stream batch (one BookKeeper entry
 * / one Pulsar message). Column-major, uncompressed at the IPC layer -- rely on Pulsar message
 * compression instead of pulling in the arrow-compression module.
 *
 * <p>Not thread-safe: a producer owns one encoder. {@link #close()} frees the off-heap allocator.
 */
public final class StreamLakeArrowBatchEncoder implements AutoCloseable {

    private final StreamLakeSchema schema;
    private final BufferAllocator allocator;

    public StreamLakeArrowBatchEncoder(StreamLakeSchema schema) {
        this.schema = schema;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    /**
     * Encode {@code rows} (each {@code Object[]} aligned to the schema columns; {@code null} cells
     * allowed) into an Arrow IPC stream byte array.
     */
    public byte[] encode(List<Object[]> rows) {
        int columnCount = schema.size();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema.toArrowSchema(), allocator)) {
            for (FieldVector vector : root.getFieldVectors()) {
                vector.setInitialCapacity(rows.size());
                vector.allocateNew();
            }
            for (int r = 0; r < rows.size(); r++) {
                Object[] row = rows.get(r);
                for (int c = 0; c < columnCount; c++) {
                    set(root.getVector(c), r, row[c]);
                }
            }
            root.setRowCount(rows.size());

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake Arrow encode failed", e);
        }
    }

    private static void set(FieldVector vector, int idx, Object value) {
        if (vector instanceof IntVector) {
            IntVector v = (IntVector) vector;
            if (value == null) {
                v.setNull(idx);
            } else {
                v.setSafe(idx, ((Number) value).intValue());
            }
        } else if (vector instanceof BigIntVector) {
            BigIntVector v = (BigIntVector) vector;
            if (value == null) {
                v.setNull(idx);
            } else {
                v.setSafe(idx, ((Number) value).longValue());
            }
        } else if (vector instanceof Float8Vector) {
            Float8Vector v = (Float8Vector) vector;
            if (value == null) {
                v.setNull(idx);
            } else {
                v.setSafe(idx, ((Number) value).doubleValue());
            }
        } else if (vector instanceof BitVector) {
            BitVector v = (BitVector) vector;
            if (value == null) {
                v.setNull(idx);
            } else {
                v.setSafe(idx, ((Boolean) value) ? 1 : 0);
            }
        } else if (vector instanceof VarCharVector) {
            VarCharVector v = (VarCharVector) vector;
            if (value == null) {
                v.setNull(idx);
            } else {
                v.setSafe(idx, new Text((String) value));
            }
        } else if (vector instanceof VarBinaryVector) {
            VarBinaryVector v = (VarBinaryVector) vector;
            if (value == null) {
                v.setNull(idx);
            } else {
                v.setSafe(idx, (byte[]) value);
            }
        } else {
            throw new IllegalArgumentException("Unsupported StreamLake vector: " + vector.getClass());
        }
    }

    @Override
    public void close() {
        allocator.close();
    }
}
