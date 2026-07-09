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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A conjunctive scan predicate over StreamLake indexed columns, evaluated against a
 * {@link StreamLakeBatchStats} (a page footer or a merged segment) to decide whether that unit
 * <i>might</i> contain matching rows. Conservative: it never excludes a unit that could match (no
 * false negatives), so surviving pages are still row-filtered on read. Column predicates combine an
 * optional order-preserving range and/or an equality/IN set; a column with no stats can't be pruned.
 */
public final class StreamLakeScanPredicate {

    /** A predicate on one column: an optional [lo, hi] range (either bound may be null) and/or IN set. */
    public static final class ColumnPredicate {
        final int columnIndex;
        final StreamLakeType type;
        final byte[] lo;
        final boolean loInclusive;
        final byte[] hi;
        final boolean hiInclusive;
        final List<byte[]> inValues;

        ColumnPredicate(int columnIndex, StreamLakeType type, byte[] lo, boolean loInclusive,
                byte[] hi, boolean hiInclusive, List<byte[]> inValues) {
            this.columnIndex = columnIndex;
            this.type = type;
            this.lo = lo;
            this.loInclusive = loInclusive;
            this.hi = hi;
            this.hiInclusive = hiInclusive;
            this.inValues = inValues;
        }

        boolean matches(StreamLakeBatchStats.ColumnStats cs) {
            if (cs == null || cs.min() == null) {
                return true; // no stats for this column -> cannot exclude
            }
            if (lo != null && Arrays.compareUnsigned(lo, cs.max()) > 0) {
                return false; // whole range is above the unit's max
            }
            if (hi != null && Arrays.compareUnsigned(hi, cs.min()) < 0) {
                return false; // whole range is below the unit's min
            }
            if (inValues != null && !inValues.isEmpty()) {
                boolean any = false;
                for (byte[] v : inValues) {
                    if (cs.mightContain(v)) {
                        any = true;
                        break;
                    }
                }
                if (!any) {
                    return false;
                }
            }
            return true;
        }

        /** Exact evaluation against a concrete decoded row value (SQL null -> excluded). */
        boolean matchesRow(Object rowValue) {
            if (rowValue == null) {
                return false;
            }
            byte[] v = StreamLakeOrderPreserving.encode(type, rowValue);
            if (lo != null) {
                int c = Arrays.compareUnsigned(v, lo);
                if (loInclusive ? c < 0 : c <= 0) {
                    return false;
                }
            }
            if (hi != null) {
                int c = Arrays.compareUnsigned(v, hi);
                if (hiInclusive ? c > 0 : c >= 0) {
                    return false;
                }
            }
            if (inValues != null && !inValues.isEmpty()) {
                boolean any = false;
                for (byte[] in : inValues) {
                    if (Arrays.equals(v, in)) {
                        any = true;
                        break;
                    }
                }
                if (!any) {
                    return false;
                }
            }
            return true;
        }
    }

    private final List<ColumnPredicate> columns;

    private StreamLakeScanPredicate(List<ColumnPredicate> columns) {
        this.columns = columns;
    }

    public List<ColumnPredicate> columns() {
        return columns;
    }

    /** Whether a unit (page footer or merged segment) might contain a matching row. */
    public boolean matches(StreamLakeBatchStats stats) {
        for (ColumnPredicate cp : columns) {
            if (!cp.matches(stats.column(cp.columnIndex))) {
                return false;
            }
        }
        return true;
    }

    /** Exact evaluation against a fully decoded row (the row filter applied after pruning). */
    public boolean matchesRow(Object[] row) {
        for (ColumnPredicate cp : columns) {
            if (!cp.matchesRow(row[cp.columnIndex])) {
                return false;
            }
        }
        return true;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder that encodes predicate values with the shared order-preserving encoding. */
    public static final class Builder {
        private final List<ColumnPredicate> columns = new ArrayList<>();

        /** A closed range on a column (both bounds inclusive); pass null for an unbounded side. */
        public Builder range(int columnIndex, StreamLakeType type, Object loInclusive, Object hiInclusive) {
            return range(columnIndex, type, loInclusive, true, hiInclusive, true);
        }

        /**
         * A range on a column with explicit bound inclusivity; pass null for an unbounded side.
         * Lets SQL {@code <}/{@code >} translate to exclusive bounds while {@code <=}/{@code >=}/BETWEEN
         * stay inclusive.
         */
        public Builder range(int columnIndex, StreamLakeType type, Object lo, boolean loInclusive,
                Object hi, boolean hiInclusive) {
            byte[] loBytes = lo == null ? null : StreamLakeOrderPreserving.encode(type, lo);
            byte[] hiBytes = hi == null ? null : StreamLakeOrderPreserving.encode(type, hi);
            columns.add(new ColumnPredicate(columnIndex, type, loBytes, loInclusive, hiBytes, hiInclusive, null));
            return this;
        }

        /** An equality predicate ({@code column = value}). */
        public Builder eq(int columnIndex, StreamLakeType type, Object value) {
            byte[] v = StreamLakeOrderPreserving.encode(type, value);
            columns.add(new ColumnPredicate(columnIndex, type, v, true, v, true,
                    java.util.Collections.singletonList(v)));
            return this;
        }

        /** An IN predicate ({@code column IN (values...)}). */
        public Builder in(int columnIndex, StreamLakeType type, List<?> values) {
            List<byte[]> encoded = new ArrayList<>(values.size());
            byte[] lo = null;
            byte[] hi = null;
            for (Object v : values) {
                byte[] e = StreamLakeOrderPreserving.encode(type, v);
                encoded.add(e);
                if (lo == null || Arrays.compareUnsigned(e, lo) < 0) {
                    lo = e;
                }
                if (hi == null || Arrays.compareUnsigned(e, hi) > 0) {
                    hi = e;
                }
            }
            columns.add(new ColumnPredicate(columnIndex, type, lo, true, hi, true, encoded));
            return this;
        }

        public StreamLakeScanPredicate build() {
            return new StreamLakeScanPredicate(new ArrayList<>(columns));
        }
    }
}
