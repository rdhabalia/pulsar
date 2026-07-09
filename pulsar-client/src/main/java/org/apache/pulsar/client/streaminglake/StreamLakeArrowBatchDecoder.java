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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
import org.apache.arrow.vector.ipc.ArrowStreamReader;

/**
 * Decodes a StreamLake Arrow IPC stream batch back into rows. The IPC stream is self-describing
 * (carries its own schema), so no external schema is required. {@link #close()} frees the allocator.
 *
 * <p>Not thread-safe: a consumer owns one decoder.
 */
public final class StreamLakeArrowBatchDecoder implements AutoCloseable {

    private final BufferAllocator allocator;

    public StreamLakeArrowBatchDecoder() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    /** Materialize all rows (each {@code Object[]} aligned to the batch columns; nulls preserved). */
    public List<Object[]> decodeRows(byte[] ipc) {
        List<Object[]> rows = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                List<FieldVector> vectors = root.getFieldVectors();
                int rowCount = root.getRowCount();
                for (int r = 0; r < rowCount; r++) {
                    Object[] row = new Object[vectors.size()];
                    for (int c = 0; c < vectors.size(); c++) {
                        row[c] = get(vectors.get(c), r);
                    }
                    rows.add(row);
                }
            }
            return rows;
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake Arrow decode failed", e);
        }
    }

    /**
     * Late materialization: decode only the given columns (in the given order), leaving the rest of
     * the columnar batch untouched. Used to read just the join-key/predicate columns first, match, and
     * fetch the remaining projected columns for surviving rows only.
     */
    public List<Object[]> decodeColumns(byte[] ipc, int[] columnIndexes) {
        List<Object[]> rows = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                int rowCount = root.getRowCount();
                FieldVector[] selected = new FieldVector[columnIndexes.length];
                for (int c = 0; c < columnIndexes.length; c++) {
                    selected[c] = root.getVector(columnIndexes[c]);
                }
                for (int r = 0; r < rowCount; r++) {
                    Object[] row = new Object[columnIndexes.length];
                    for (int c = 0; c < selected.length; c++) {
                        row[c] = get(selected[c], r);
                    }
                    rows.add(row);
                }
            }
            return rows;
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake Arrow decode failed", e);
        }
    }

    /** Late materialization of a single column as one value per row (nulls preserved). */
    public List<Object> decodeColumn(byte[] ipc, int columnIndex) {
        List<Object> values = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                FieldVector v = root.getVector(columnIndex);
                int rowCount = root.getRowCount();
                for (int r = 0; r < rowCount; r++) {
                    values.add(get(v, r));
                }
            }
            return values;
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake Arrow decode failed", e);
        }
    }

    /**
     * Open a single Arrow batch for <b>late materialization</b>: read the cheap key/predicate cells
     * first ({@link Batch#value}) and materialize a full row ({@link Batch#row}) only for the rows that
     * survive — so an inner join never allocates the wide columns of non-matching rows. A StreamLake
     * payload holds exactly one batch. The returned {@link Batch} must be closed.
     */
    public Batch open(byte[] ipc) {
        try {
            ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator);
            if (!reader.loadNextBatch()) {
                reader.close();
                return new Batch(null, null, 0);
            }
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            return new Batch(reader, root, root.getRowCount());
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake Arrow open failed", e);
        }
    }

    /** One loaded Arrow batch; cells are read on demand from the live vectors until {@link #close()}. */
    public static final class Batch implements AutoCloseable {
        private final ArrowStreamReader reader;
        private final VectorSchemaRoot root;
        private final int rowCount;
        private final int columnCount;

        Batch(ArrowStreamReader reader, VectorSchemaRoot root, int rowCount) {
            this.reader = reader;
            this.root = root;
            this.rowCount = rowCount;
            this.columnCount = root == null ? 0 : root.getFieldVectors().size();
        }

        public int rowCount() {
            return rowCount;
        }

        public int columnCount() {
            return columnCount;
        }

        /** One cell (cheap: read the single vector slot). */
        public Object value(int row, int column) {
            return get(root.getVector(column), row);
        }

        /** Materialize the full row (all columns) -- call only for rows that survive the filter/probe. */
        public Object[] row(int row) {
            Object[] r = new Object[columnCount];
            for (int c = 0; c < columnCount; c++) {
                r[c] = get(root.getVector(c), row);
            }
            return r;
        }

        @Override
        public void close() {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ignore) {
                    // best-effort
                }
            }
        }
    }

    private static Object get(FieldVector vector, int idx) {
        if (vector.isNull(idx)) {
            return null;
        }
        if (vector instanceof IntVector) {
            return ((IntVector) vector).get(idx);
        } else if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(idx);
        } else if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(idx);
        } else if (vector instanceof BitVector) {
            return ((BitVector) vector).get(idx) != 0;
        } else if (vector instanceof VarCharVector) {
            return new String(((VarCharVector) vector).get(idx), StandardCharsets.UTF_8);
        } else if (vector instanceof VarBinaryVector) {
            return ((VarBinaryVector) vector).get(idx);
        }
        throw new IllegalArgumentException("Unsupported StreamLake vector: " + vector.getClass());
    }

    @Override
    public void close() {
        allocator.close();
    }
}
