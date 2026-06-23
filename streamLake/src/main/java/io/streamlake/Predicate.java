package io.streamlake;

import java.util.ArrayList;
import java.util.List;

/**
 * A predicate tree evaluated at the broker for exact, row-level filtering.
 *
 * <p>Two responsibilities:
 * <ul>
 *   <li>{@link #eval(RowAccessor)} — exact boolean evaluation against a row.</li>
 *   <li>{@link #extractColumnPredicates} — a conservative projection into per-column
 *       order-preserving ranges, which is what the bookie uses to prune pages. This is
 *       a deliberately simple planner: it handles AND-of-(comparison | OR-of-comparisons
 *       on the same column), which covers the common analytics predicate shape
 *       (e.g. {@code deptId > 1 AND (salary < 50 OR salary > 100)}).</li>
 * </ul>
 */
public abstract class Predicate {

    public enum Op { GT, GE, LT, LE, EQ }

    public abstract boolean eval(RowAccessor row);

    // ---- factory helpers -------------------------------------------------

    public static Predicate gt(short col, ColumnType t, Object v) {
        return new Compare(col, t, Op.GT, v);
    }

    public static Predicate ge(short col, ColumnType t, Object v) {
        return new Compare(col, t, Op.GE, v);
    }

    public static Predicate lt(short col, ColumnType t, Object v) {
        return new Compare(col, t, Op.LT, v);
    }

    public static Predicate le(short col, ColumnType t, Object v) {
        return new Compare(col, t, Op.LE, v);
    }

    public static Predicate eq(short col, ColumnType t, Object v) {
        return new Compare(col, t, Op.EQ, v);
    }

    public static Predicate and(Predicate... children) {
        return new And(children);
    }

    public static Predicate or(Predicate... children) {
        return new Or(children);
    }

    /**
     * Conservative pruning projection: returns one entry per column referenced,
     * each holding the OR-set of candidate ranges for that column. The bookie keeps
     * a page iff, for EVERY returned column, the page's range overlaps at least one
     * candidate range (AND across columns, OR within a column). Conservative ⇒ never
     * drops a page that could contain a matching row.
     */
    public List<ColumnPredicateRanges> extractColumnPredicates() {
        List<ColumnPredicateRanges> out = new ArrayList<>();
        collectInto(this, out);
        return out;
    }

    private static void collectInto(Predicate p, List<ColumnPredicateRanges> out) {
        if (p instanceof And) {
            for (Predicate c : ((And) p).children) {
                collectInto(c, out);
            }
        } else if (p instanceof Or) {
            // OR of comparisons on a single column -> union of ranges for that column.
            Or or = (Or) p;
            Short col = null;
            ColumnType type = null;
            List<ColumnRange> ranges = new ArrayList<>();
            boolean singleColumn = true;
            for (Predicate c : or.children) {
                if (!(c instanceof Compare)) {
                    singleColumn = false;
                    break;
                }
                Compare cmp = (Compare) c;
                if (col == null) {
                    col = cmp.columnId;
                    type = cmp.type;
                } else if (col != cmp.columnId) {
                    singleColumn = false;
                    break;
                }
                ranges.add(cmp.toRange());
            }
            if (singleColumn && col != null) {
                out.add(new ColumnPredicateRanges(col, ranges));
            }
            // If the OR spans multiple columns we simply skip pruning it (still correct,
            // just less selective) — the exact eval at the broker handles correctness.
        } else if (p instanceof Compare) {
            Compare cmp = (Compare) p;
            List<ColumnRange> single = new ArrayList<>();
            single.add(cmp.toRange());
            out.add(new ColumnPredicateRanges(cmp.columnId, single));
        }
    }

    // ---- node types ------------------------------------------------------

    /** Per-column OR-set of candidate ranges, shipped to the bookie for pruning. */
    public static final class ColumnPredicateRanges {
        public final short columnId;
        public final List<ColumnRange> orRanges;

        public ColumnPredicateRanges(short columnId, List<ColumnRange> orRanges) {
            this.columnId = columnId;
            this.orRanges = orRanges;
        }
    }

    public static final class Compare extends Predicate {
        public final short columnId;
        public final ColumnType type;
        public final Op op;
        public final Object value;

        public Compare(short columnId, ColumnType type, Op op, Object value) {
            this.columnId = columnId;
            this.type = type;
            this.op = op;
            this.value = value;
        }

        @Override
        public boolean eval(RowAccessor row) {
            int c = compare(row.get(columnId), value, type);
            switch (op) {
                case GT: return c > 0;
                case GE: return c >= 0;
                case LT: return c < 0;
                case LE: return c <= 0;
                case EQ: return c == 0;
                default: throw new IllegalStateException();
            }
        }

        /** Translate the comparison into an order-preserving range for pruning. */
        ColumnRange toRange() {
            byte[] enc = type.encodeOrderPreserving(value);
            switch (op) {
                case GT:
                    return new ColumnRange(columnId, enc, null, true, false);   // (value, +inf)
                case GE:
                    return new ColumnRange(columnId, enc, null, false, false);  // [value, +inf)
                case LT:
                    return new ColumnRange(columnId, null, enc, false, true);   // (-inf, value)
                case LE:
                    return new ColumnRange(columnId, null, enc, false, false);  // (-inf, value]
                case EQ:
                    return new ColumnRange(columnId, enc, enc, false, false);   // [value, value]
                default:
                    throw new IllegalStateException();
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static int compare(Object a, Object b, ColumnType t) {
            switch (t) {
                case INT:
                    return Integer.compare(((Number) a).intValue(), ((Number) b).intValue());
                case LONG:
                    return Long.compare(((Number) a).longValue(), ((Number) b).longValue());
                case STRING:
                    return ((String) a).compareTo((String) b);
                default:
                    throw new IllegalStateException();
            }
        }
    }

    public static final class And extends Predicate {
        public final Predicate[] children;

        public And(Predicate[] children) {
            this.children = children;
        }

        @Override
        public boolean eval(RowAccessor row) {
            for (Predicate c : children) {
                if (!c.eval(row)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static final class Or extends Predicate {
        public final Predicate[] children;

        public Or(Predicate[] children) {
            this.children = children;
        }

        @Override
        public boolean eval(RowAccessor row) {
            for (Predicate c : children) {
                if (c.eval(row)) {
                    return true;
                }
            }
            return false;
        }
    }
}
