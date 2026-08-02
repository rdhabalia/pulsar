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
import java.util.function.Function;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
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

    /** Either a single-table {@link Plan} or a two-table {@link JoinPlan}. */
    public static final class Planned {
        private final Plan single;
        private final JoinPlan join;

        private Planned(Plan single, JoinPlan join) {
            this.single = single;
            this.join = join;
        }

        public boolean isJoin() {
            return join != null;
        }

        public Plan single() {
            return single;
        }

        public JoinPlan join() {
            return join;
        }
    }

    /**
     * A translated inner equi-join of two StreamLake tables: a pushed-down predicate + join key for each
     * side, plus a projection over the combined schema (left columns first, then right columns). The
     * coordinator runs {@code leftExecutor.scanInnerJoin(left, right)} and {@link #combine} reorders the
     * executor's {@code concat(right,left)} rows into natural {@code [left..., right...]} order and
     * applies the SELECT projection.
     */
    public static final class JoinPlan {
        private final String leftTable;
        private final String rightTable;
        private final StreamLakeScanPredicate leftPredicate;
        private final StreamLakeScanPredicate rightPredicate;
        private final int leftKey;
        private final int rightKey;
        private final int leftWidth;
        private final int rightWidth;
        private final int[] projection; // into combined [left..., right...]; null == all
        private final List<String> columnNames;

        JoinPlan(String leftTable, String rightTable, StreamLakeScanPredicate leftPredicate,
                StreamLakeScanPredicate rightPredicate, int leftKey, int rightKey, int leftWidth,
                int rightWidth, int[] projection, List<String> columnNames) {
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            this.leftPredicate = leftPredicate;
            this.rightPredicate = rightPredicate;
            this.leftKey = leftKey;
            this.rightKey = rightKey;
            this.leftWidth = leftWidth;
            this.rightWidth = rightWidth;
            this.projection = projection;
            this.columnNames = columnNames;
        }

        public String leftTable() {
            return leftTable;
        }

        public String rightTable() {
            return rightTable;
        }

        public StreamLakeScanPredicate leftPredicate() {
            return leftPredicate;
        }

        public StreamLakeScanPredicate rightPredicate() {
            return rightPredicate;
        }

        public int leftKey() {
            return leftKey;
        }

        public int rightKey() {
            return rightKey;
        }

        /** Result header, in projected order (qualified {@code table.column} names). */
        public List<String> columnNames() {
            return columnNames;
        }

        /**
         * Reorder the executor's {@code concat(rightRow, leftRow)} join rows into natural
         * {@code [left..., right...]} order and apply the SELECT projection.
         */
        public List<Object[]> combine(List<Object[]> concatRows) {
            List<Object[]> out = new ArrayList<>(concatRows.size());
            for (Object[] c : concatRows) {
                Object[] natural = new Object[leftWidth + rightWidth];
                System.arraycopy(c, rightWidth, natural, 0, leftWidth);   // left columns
                System.arraycopy(c, 0, natural, leftWidth, rightWidth);   // right columns
                if (projection == null) {
                    out.add(natural);
                } else {
                    Object[] p = new Object[projection.length];
                    for (int i = 0; i < projection.length; i++) {
                        p[i] = natural[projection[i]];
                    }
                    out.add(p);
                }
            }
            return out;
        }
    }

    /**
     * Plan a statement that may be a single-table query or a two-table inner equi-join. Table schemas
     * (and optional per-table time columns) are resolved by name via the supplied functions, so the
     * caller (broker coordinator) can look each table's schema up from its topic policy.
     */
    public static Planned planStatement(String sql, Function<String, StreamLakeSchema> schemaByTable,
            Function<String, String> timeColByTable) {
        SqlNode parsed = parse(sql);
        SqlSelect select = parsed instanceof SqlOrderBy ? asSelect(((SqlOrderBy) parsed).query)
                : asSelect(parsed);
        if (select.getFrom() instanceof SqlJoin) {
            if (parsed instanceof SqlOrderBy) {
                throw new IllegalArgumentException("ORDER BY / LIMIT is not supported for joins yet");
            }
            return new Planned(null, planJoin(select, (SqlJoin) select.getFrom(), schemaByTable));
        }
        String table = tableName(select.getFrom());
        StreamLakeSchema schema = schemaByTable.apply(table);
        if (schema == null) {
            throw new IllegalArgumentException("Unknown table: " + table);
        }
        return new Planned(plan(sql, schema, timeColByTable.apply(table)), null);
    }

    private static SqlSelect asSelect(SqlNode node) {
        if (node instanceof SqlSelect) {
            return (SqlSelect) node;
        }
        throw new IllegalArgumentException("Only SELECT queries are supported, got: " + node.getKind());
    }

    // Translate SELECT <cols|*> FROM a JOIN b ON a.k=b.k WHERE <qualified conjuncts>.
    private static JoinPlan planJoin(SqlSelect select, SqlJoin join,
            Function<String, StreamLakeSchema> schemaByTable) {
        if (join.getJoinType() != JoinType.INNER && join.getJoinType() != JoinType.COMMA) {
            throw new IllegalArgumentException("Only INNER JOIN is supported, got: " + join.getJoinType());
        }
        Side left = side(join.getLeft(), schemaByTable, true);
        Side right = side(join.getRight(), schemaByTable, false);
        Map<String, Side> byAlias = new HashMap<>();
        byAlias.put(left.alias.toLowerCase(Locale.ROOT), left);
        byAlias.put(left.table.toLowerCase(Locale.ROOT), left);
        byAlias.put(right.alias.toLowerCase(Locale.ROOT), right);
        byAlias.put(right.table.toLowerCase(Locale.ROOT), right);

        // ON a.k = b.k -> one key column per side.
        if (join.getCondition() == null || join.getCondition().getKind() != SqlKind.EQUALS) {
            throw new IllegalArgumentException("JOIN requires an ON <left>.<col> = <right>.<col> condition");
        }
        SqlCall on = (SqlCall) join.getCondition();
        SideColumn onLeft = resolveColumn(on.operand(0), byAlias, left, right);
        SideColumn onRight = resolveColumn(on.operand(1), byAlias, left, right);
        if (onLeft.side == onRight.side) {
            throw new IllegalArgumentException("JOIN ON must reference one column from each table");
        }
        int leftKey = (onLeft.side == left ? onLeft : onRight).index;
        int rightKey = (onLeft.side == right ? onLeft : onRight).index;

        StreamLakeScanPredicate.Builder leftPred = StreamLakeScanPredicate.builder();
        StreamLakeScanPredicate.Builder rightPred = StreamLakeScanPredicate.builder();
        if (select.getWhere() != null) {
            translateJoinWhere(select.getWhere(), byAlias, left, right, leftPred, rightPred);
        }

        int leftWidth = left.schema.columns().size();
        int rightWidth = right.schema.columns().size();
        int[] projection = joinProjection(select.getSelectList(), byAlias, left, right, leftWidth);
        List<String> names = joinColumnNames(projection, left, right, leftWidth, rightWidth);
        return new JoinPlan(left.table, right.table, leftPred.build(), rightPred.build(), leftKey,
                rightKey, leftWidth, rightWidth, projection, names);
    }

    private static Side side(SqlNode node, Function<String, StreamLakeSchema> schemaByTable, boolean isLeft) {
        String table;
        String alias;
        if (node.getKind() == SqlKind.AS) {
            SqlBasicCall as = (SqlBasicCall) node;
            table = identifierName(as.operand(0));
            alias = identifierName(as.operand(1));
        } else if (node instanceof SqlIdentifier) {
            table = identifierName(node);
            alias = table;
        } else {
            throw new IllegalArgumentException("JOIN side must be a table (optionally aliased)");
        }
        StreamLakeSchema schema = schemaByTable.apply(table);
        if (schema == null) {
            throw new IllegalArgumentException("Unknown table: " + table);
        }
        return new Side(table, alias, schema, new ColumnResolver(schema), isLeft);
    }

    private static void translateJoinWhere(SqlNode where, Map<String, Side> byAlias, Side left, Side right,
            StreamLakeScanPredicate.Builder leftPred, StreamLakeScanPredicate.Builder rightPred) {
        SqlKind kind = where.getKind();
        if (kind == SqlKind.AND) {
            for (SqlNode operand : ((SqlCall) where).getOperandList()) {
                translateJoinWhere(operand, byAlias, left, right, leftPred, rightPred);
            }
            return;
        }
        SqlCall call = (SqlCall) where;
        switch (kind) {
            case EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL: {
                SqlNode a = call.operand(0);
                SqlNode b = call.operand(1);
                SqlKind effective = kind;
                SqlNode colNode;
                SqlLiteral literal;
                if (a instanceof SqlIdentifier && b instanceof SqlLiteral) {
                    colNode = a;
                    literal = (SqlLiteral) b;
                } else if (b instanceof SqlIdentifier && a instanceof SqlLiteral) {
                    colNode = b;
                    literal = (SqlLiteral) a;
                    effective = flip(kind);
                } else {
                    throw new IllegalArgumentException("Comparison must be between a column and a literal");
                }
                SideColumn sc = resolveColumn(colNode, byAlias, left, right);
                StreamLakeType type = sc.side.cols.type(sc.index);
                applyComparison(effective, sc.index, type, literalValue(literal, type),
                        sc.side.isLeft ? leftPred : rightPred);
                return;
            }
            case BETWEEN: {
                List<SqlNode> ops = call.getOperandList();
                SideColumn sc = resolveColumn(ops.get(0), byAlias, left, right);
                if (!(ops.get(1) instanceof SqlLiteral) || !(ops.get(2) instanceof SqlLiteral)) {
                    throw new IllegalArgumentException("BETWEEN must compare a column to two literals");
                }
                StreamLakeType type = sc.side.cols.type(sc.index);
                (sc.side.isLeft ? leftPred : rightPred).range(sc.index, type,
                        literalValue((SqlLiteral) ops.get(1), type), true,
                        literalValue((SqlLiteral) ops.get(2), type), true);
                return;
            }
            case IN: {
                List<SqlNode> ops = call.getOperandList();
                SideColumn sc = resolveColumn(ops.get(0), byAlias, left, right);
                if (!(ops.get(1) instanceof SqlNodeList)) {
                    throw new IllegalArgumentException("IN must be a column against a value list");
                }
                StreamLakeType type = sc.side.cols.type(sc.index);
                List<Object> values = new ArrayList<>();
                for (SqlNode v : (SqlNodeList) ops.get(1)) {
                    values.add(literalValue((SqlLiteral) v, type));
                }
                (sc.side.isLeft ? leftPred : rightPred).in(sc.index, type, values);
                return;
            }
            default:
                throw new IllegalArgumentException("Unsupported WHERE clause: " + kind);
        }
    }

    private static int[] joinProjection(SqlNodeList selectList, Map<String, Side> byAlias, Side left,
            Side right, int leftWidth) {
        if (selectList == null || (selectList.size() == 1 && selectList.get(0) instanceof SqlIdentifier
                && ((SqlIdentifier) selectList.get(0)).isStar())) {
            return null; // SELECT * -> all combined columns
        }
        int[] proj = new int[selectList.size()];
        for (int i = 0; i < selectList.size(); i++) {
            SideColumn sc = resolveColumn(selectList.get(i), byAlias, left, right);
            proj[i] = sc.side.isLeft ? sc.index : leftWidth + sc.index;
        }
        return proj;
    }

    private static List<String> joinColumnNames(int[] projection, Side left, Side right, int leftWidth,
            int rightWidth) {
        List<String> all = new ArrayList<>(leftWidth + rightWidth);
        for (StreamLakeSchema.Column c : left.schema.columns()) {
            all.add(left.table + "." + c.name());
        }
        for (StreamLakeSchema.Column c : right.schema.columns()) {
            all.add(right.table + "." + c.name());
        }
        if (projection == null) {
            return all;
        }
        List<String> out = new ArrayList<>(projection.length);
        for (int idx : projection) {
            out.add(all.get(idx));
        }
        return out;
    }

    // Resolve a (possibly qualified) column identifier to its side + column index.
    private static SideColumn resolveColumn(SqlNode node, Map<String, Side> byAlias, Side left, Side right) {
        if (!(node instanceof SqlIdentifier)) {
            throw new IllegalArgumentException("Expected a column reference");
        }
        SqlIdentifier id = (SqlIdentifier) node;
        String column = id.names.get(id.names.size() - 1);
        String qualifier = id.names.size() >= 2 ? id.names.get(id.names.size() - 2) : null;
        Side side;
        if (qualifier != null) {
            side = byAlias.get(qualifier.toLowerCase(Locale.ROOT));
            if (side == null) {
                throw new IllegalArgumentException("Unknown table/alias: " + qualifier);
            }
        } else {
            boolean inLeft = left.cols.has(column);
            boolean inRight = right.cols.has(column);
            if (inLeft && inRight) {
                throw new IllegalArgumentException("Ambiguous column '" + column + "'; qualify it with a table");
            }
            side = inLeft ? left : inRight ? right : null;
            if (side == null) {
                throw new IllegalArgumentException("Unknown column: " + column);
            }
        }
        return new SideColumn(side, side.cols.index(column));
    }

    private static final class Side {
        private final String table;
        private final String alias;
        private final StreamLakeSchema schema;
        private final ColumnResolver cols;
        private final boolean isLeft;

        Side(String table, String alias, StreamLakeSchema schema, ColumnResolver cols, boolean isLeft) {
            this.table = table;
            this.alias = alias;
            this.schema = schema;
            this.cols = cols;
            this.isLeft = isLeft;
        }
    }

    private static final class SideColumn {
        private final Side side;
        private final int index;

        SideColumn(Side side, int index) {
            this.side = side;
            this.index = index;
        }
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
        applyComparison(effective, idx, type, value, predicate);
    }

    // Apply a column<op>literal comparison to a predicate builder (shared by single-table and join).
    private static void applyComparison(SqlKind effective, int idx, StreamLakeType type, Object value,
            StreamLakeScanPredicate.Builder predicate) {
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

        boolean has(String name) {
            return byName.containsKey(name.toLowerCase(Locale.ROOT));
        }

        StreamLakeType type(int index) {
            return schema.columns().get(index).type();
        }
    }
}
