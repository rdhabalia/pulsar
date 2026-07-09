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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeType;

/**
 * The StreamLake SQL frontend: parses a SQL query with Apache Calcite and translates it into a
 * {@link StreamLakeQueryExecutor}-ready {@link Plan} (a pushed-down {@link StreamLakeScanPredicate},
 * a projection, an ORDER BY / LIMIT top-K, and an optional event-time window). This is the "Calcite"
 * half of the query tier: Calcite owns SQL parsing (and its AST), while StreamLake owns pruning and
 * columnar execution. Only the analytic subset the pruner/executor can honor is accepted:
 *
 * <ul>
 *   <li>{@code SELECT <cols>|*} projection over the registered {@link StreamLakeSchema};</li>
 *   <li>{@code FROM <topic>} (single table);</li>
 *   <li>a conjunctive ({@code AND}-only) {@code WHERE} of {@code =, &gt;, &gt;=, &lt;, &lt;=,
 *       BETWEEN, IN} comparing an indexed column to literals (either operand order);</li>
 *   <li>{@code ORDER BY <col> [ASC|DESC]} with an optional {@code LIMIT k} (bounded top-K).</li>
 * </ul>
 *
 * <p>Predicates on the configured {@code timeColumn} become the executor's [fromMs, toMs] date-prune
 * window and are dropped from the row predicate (date pruning already handles them). Column names are
 * resolved case-insensitively; unquoted identifiers keep their original case.
 */
public final class StreamLakeSqlPlanner {

    private StreamLakeSqlPlanner() {
    }

    /** A translated, executor-ready query plan. Fields are final; use the getters. */
    public static final class Plan {
        private final String table;
        private final StreamLakeScanPredicate predicate;
        private final int[] projection; // null == SELECT * (all columns, schema order)
        private final int sortColumn;   // -1 == no ORDER BY
        private final boolean descending;
        private final int limit;        // -1 == no LIMIT
        private final long fromMs;
        private final long toMs;

        Plan(String table, StreamLakeScanPredicate predicate, int[] projection, int sortColumn,
                boolean descending, int limit, long fromMs, long toMs) {
            this.table = table;
            this.predicate = predicate;
            this.projection = projection;
            this.sortColumn = sortColumn;
            this.descending = descending;
            this.limit = limit;
            this.fromMs = fromMs;
            this.toMs = toMs;
        }

        public String table() {
            return table;
        }

        public StreamLakeScanPredicate predicate() {
            return predicate;
        }

        /** Column indexes (into the full schema) to emit, in SELECT order; null means all columns. */
        public int[] projection() {
            return projection == null ? null : projection.clone();
        }

        public int sortColumn() {
            return sortColumn;
        }

        public boolean descending() {
            return descending;
        }

        public int limit() {
            return limit;
        }

        public long fromMs() {
            return fromMs;
        }

        public long toMs() {
            return toMs;
        }

        /** Apply the SELECT projection to full scan rows (identity for {@code SELECT *}). */
        public List<Object[]> project(List<Object[]> rows) {
            if (projection == null) {
                return rows;
            }
            List<Object[]> out = new ArrayList<>(rows.size());
            for (Object[] r : rows) {
                Object[] p = new Object[projection.length];
                for (int i = 0; i < projection.length; i++) {
                    p[i] = r[projection[i]];
                }
                out.add(p);
            }
            return out;
        }
    }

    public static Plan plan(String sql, StreamLakeSchema schema) {
        return plan(sql, schema, null);
    }

    /**
     * Parse and translate {@code sql} against {@code schema}. If {@code timeColumn} is non-null,
     * WHERE predicates on that column drive the date-prune window instead of the row predicate.
     */
    public static Plan plan(String sql, StreamLakeSchema schema, String timeColumn) {
        SqlNode parsed = parse(sql);

        SqlSelect select;
        SqlNodeList orderList = null;
        SqlNode fetch = null;
        if (parsed instanceof SqlOrderBy) {
            SqlOrderBy orderBy = (SqlOrderBy) parsed;
            if (!(orderBy.query instanceof SqlSelect)) {
                throw new IllegalArgumentException("Only SELECT queries are supported");
            }
            select = (SqlSelect) orderBy.query;
            orderList = orderBy.orderList;
            fetch = orderBy.fetch;
        } else if (parsed instanceof SqlSelect) {
            select = (SqlSelect) parsed;
            orderList = select.getOrderList();
            fetch = select.getFetch();
        } else {
            throw new IllegalArgumentException("Only SELECT queries are supported, got: " + parsed.getKind());
        }

        ColumnResolver columns = new ColumnResolver(schema);
        String timeCol = timeColumn == null ? null : timeColumn.toLowerCase(Locale.ROOT);

        String table = tableName(select.getFrom());
        int[] projection = projection(select.getSelectList(), columns);

        long[] window = {Long.MIN_VALUE, Long.MAX_VALUE};
        StreamLakeScanPredicate.Builder predicate = StreamLakeScanPredicate.builder();
        if (select.getWhere() != null) {
            translateWhere(select.getWhere(), columns, timeCol, predicate, window);
        }

        int sortColumn = -1;
        boolean descending = false;
        if (orderList != null && orderList.size() > 0) {
            if (orderList.size() != 1) {
                throw new IllegalArgumentException("ORDER BY supports a single column");
            }
            SqlNode item = orderList.get(0);
            if (item.getKind() == SqlKind.DESCENDING) {
                descending = true;
                item = ((SqlCall) item).operand(0);
            }
            sortColumn = columns.index(identifierName(item));
        }

        int limit = -1;
        if (fetch instanceof SqlLiteral) {
            limit = ((SqlLiteral) fetch).getValueAs(Integer.class);
        }

        return new Plan(table, predicate.build(), projection, sortColumn, descending, limit,
                window[0], window[1]);
    }

    private static SqlNode parse(String sql) {
        SqlParser.Config config = SqlParser.config()
                .withUnquotedCasing(Casing.UNCHANGED)
                .withQuotedCasing(Casing.UNCHANGED)
                .withCaseSensitive(true);
        try {
            return SqlParser.create(sql, config).parseQuery();
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            throw new IllegalArgumentException("Failed to parse SQL: " + e.getMessage(), e);
        }
    }

    private static String tableName(SqlNode from) {
        if (!(from instanceof SqlIdentifier)) {
            throw new IllegalArgumentException("FROM must be a single table name");
        }
        return identifierName(from);
    }

    private static int[] projection(SqlNodeList selectList, ColumnResolver columns) {
        if (selectList == null) {
            return null;
        }
        // SELECT * -> null projection (all columns in schema order).
        if (selectList.size() == 1 && selectList.get(0) instanceof SqlIdentifier
                && ((SqlIdentifier) selectList.get(0)).isStar()) {
            return null;
        }
        int[] proj = new int[selectList.size()];
        for (int i = 0; i < selectList.size(); i++) {
            SqlNode item = selectList.get(i);
            if (!(item instanceof SqlIdentifier)) {
                throw new IllegalArgumentException("SELECT supports bare column references only");
            }
            proj[i] = columns.index(identifierName(item));
        }
        return proj;
    }

    // Walk a conjunctive WHERE, appending each comparison to the predicate builder (or, for the time
    // column, narrowing the [fromMs, toMs] window in place).
    private static void translateWhere(SqlNode where, ColumnResolver columns, String timeCol,
            StreamLakeScanPredicate.Builder predicate, long[] window) {
        SqlKind kind = where.getKind();
        if (kind == SqlKind.AND) {
            for (SqlNode operand : ((SqlCall) where).getOperandList()) {
                translateWhere(operand, columns, timeCol, predicate, window);
            }
            return;
        }
        SqlCall call = (SqlCall) where;
        switch (kind) {
            case EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                translateComparison(kind, call, columns, timeCol, predicate, window);
                return;
            case BETWEEN:
                translateBetween(call, columns, timeCol, predicate, window);
                return;
            case IN:
                translateIn(call, columns, predicate);
                return;
            default:
                throw new IllegalArgumentException("Unsupported WHERE clause: " + kind);
        }
    }

    private static void translateComparison(SqlKind kind, SqlCall call, ColumnResolver columns,
            String timeCol, StreamLakeScanPredicate.Builder predicate, long[] window) {
        SqlNode left = call.operand(0);
        SqlNode right = call.operand(1);
        SqlIdentifier col;
        SqlLiteral literal;
        SqlKind effective = kind;
        if (left instanceof SqlIdentifier && right instanceof SqlLiteral) {
            col = (SqlIdentifier) left;
            literal = (SqlLiteral) right;
        } else if (right instanceof SqlIdentifier && left instanceof SqlLiteral) {
            // literal <op> column: flip the operator so it reads column <op'> literal.
            col = (SqlIdentifier) right;
            literal = (SqlLiteral) left;
            effective = flip(kind);
        } else {
            throw new IllegalArgumentException("Comparison must be between a column and a literal");
        }

        String name = identifierName(col);
        if (timeCol != null && name.toLowerCase(Locale.ROOT).equals(timeCol)) {
            narrowWindow(effective, ((Number) literal.getValueAs(Long.class)).longValue(), window);
            return;
        }
        int idx = columns.index(name);
        StreamLakeType type = columns.type(idx);
        Object value = literalValue(literal, type);
        switch (effective) {
            case EQUALS:
                predicate.eq(idx, type, value);
                return;
            case GREATER_THAN:
                predicate.range(idx, type, value, false, null, true);
                return;
            case GREATER_THAN_OR_EQUAL:
                predicate.range(idx, type, value, true, null, true);
                return;
            case LESS_THAN:
                predicate.range(idx, type, null, true, value, false);
                return;
            case LESS_THAN_OR_EQUAL:
                predicate.range(idx, type, null, true, value, true);
                return;
            default:
                throw new IllegalArgumentException("Unsupported comparison: " + effective);
        }
    }

    private static void translateBetween(SqlCall call, ColumnResolver columns, String timeCol,
            StreamLakeScanPredicate.Builder predicate, long[] window) {
        List<SqlNode> operands = call.getOperandList();
        SqlNode colNode = operands.get(0);
        SqlNode loNode = operands.get(1);
        SqlNode hiNode = operands.get(2);
        if (!(colNode instanceof SqlIdentifier) || !(loNode instanceof SqlLiteral)
                || !(hiNode instanceof SqlLiteral)) {
            throw new IllegalArgumentException("BETWEEN must compare a column to two literals");
        }
        String name = identifierName(colNode);
        if (timeCol != null && name.toLowerCase(Locale.ROOT).equals(timeCol)) {
            narrowWindow(SqlKind.GREATER_THAN_OR_EQUAL,
                    ((Number) ((SqlLiteral) loNode).getValueAs(Long.class)).longValue(), window);
            narrowWindow(SqlKind.LESS_THAN_OR_EQUAL,
                    ((Number) ((SqlLiteral) hiNode).getValueAs(Long.class)).longValue(), window);
            return;
        }
        int idx = columns.index(name);
        StreamLakeType type = columns.type(idx);
        predicate.range(idx, type, literalValue((SqlLiteral) loNode, type), true,
                literalValue((SqlLiteral) hiNode, type), true);
    }

    private static void translateIn(SqlCall call, ColumnResolver columns,
            StreamLakeScanPredicate.Builder predicate) {
        List<SqlNode> operands = call.getOperandList();
        SqlNode colNode = operands.get(0);
        SqlNode valuesNode = operands.get(1);
        if (!(colNode instanceof SqlIdentifier) || !(valuesNode instanceof SqlNodeList)) {
            throw new IllegalArgumentException("IN must be a column against a value list");
        }
        int idx = columns.index(identifierName(colNode));
        StreamLakeType type = columns.type(idx);
        List<Object> values = new ArrayList<>();
        for (SqlNode v : (SqlNodeList) valuesNode) {
            if (!(v instanceof SqlLiteral)) {
                throw new IllegalArgumentException("IN list supports literals only");
            }
            values.add(literalValue((SqlLiteral) v, type));
        }
        predicate.in(idx, type, values);
    }

    private static void narrowWindow(SqlKind kind, long value, long[] window) {
        switch (kind) {
            case EQUALS:
                window[0] = Math.max(window[0], value);
                window[1] = Math.min(window[1], value);
                return;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                window[0] = Math.max(window[0], value);
                return;
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                window[1] = Math.min(window[1], value);
                return;
            default:
                throw new IllegalArgumentException("Unsupported time predicate: " + kind);
        }
    }

    private static SqlKind flip(SqlKind kind) {
        switch (kind) {
            case GREATER_THAN:
                return SqlKind.LESS_THAN;
            case GREATER_THAN_OR_EQUAL:
                return SqlKind.LESS_THAN_OR_EQUAL;
            case LESS_THAN:
                return SqlKind.GREATER_THAN;
            case LESS_THAN_OR_EQUAL:
                return SqlKind.GREATER_THAN_OR_EQUAL;
            default:
                return kind; // EQUALS is symmetric
        }
    }

    private static Object literalValue(SqlLiteral literal, StreamLakeType type) {
        switch (type) {
            case INT32:
                return literal.getValueAs(Integer.class);
            case INT64:
                return literal.getValueAs(Long.class);
            case DOUBLE:
                return literal.getValueAs(Double.class);
            case BOOLEAN:
                return literal.getValueAs(Boolean.class);
            case STRING:
                return literal.getValueAs(String.class);
            case BYTES:
                return literal.getValueAs(String.class).getBytes(StandardCharsets.UTF_8);
            default:
                throw new IllegalArgumentException("Unsupported column type: " + type);
        }
    }

    private static String identifierName(SqlNode node) {
        if (!(node instanceof SqlIdentifier)) {
            throw new IllegalArgumentException("Expected a column/table identifier");
        }
        SqlIdentifier id = (SqlIdentifier) node;
        return id.isSimple() ? id.getSimple() : id.names.get(id.names.size() - 1);
    }

    /** Case-insensitive resolution of column names to their schema index and type. */
    private static final class ColumnResolver {
        private final StreamLakeSchema schema;
        private final Map<String, Integer> byName = new HashMap<>();

        ColumnResolver(StreamLakeSchema schema) {
            this.schema = schema;
            List<StreamLakeSchema.Column> cols = schema.columns();
            for (int i = 0; i < cols.size(); i++) {
                byName.put(cols.get(i).name().toLowerCase(Locale.ROOT), i);
            }
        }

        int index(String name) {
            Integer idx = byName.get(name.toLowerCase(Locale.ROOT));
            if (idx == null) {
                throw new IllegalArgumentException("Unknown column: " + name);
            }
            return idx;
        }

        StreamLakeType type(int index) {
            return schema.columns().get(index).type();
        }
    }
}
