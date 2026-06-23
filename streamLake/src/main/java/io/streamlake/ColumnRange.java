package io.streamlake;

/**
 * Min/max range for one indexed column, encoded as <b>order-preserving bytes</b>.
 *
 * <p>This is the opaque blob the broker ships alongside the entry and the bookie
 * stores in the page-range index. The bookie evaluates {@link #overlaps} using
 * pure lexicographic {@code byte[]} comparison — it never decodes values, learns
 * the column type, or interprets schema. {@code null} min == -infinity, {@code null}
 * max == +infinity (used to express open-ended predicate ranges like {@code > 100}).
 */
public final class ColumnRange {
    public final short columnId;
    public final byte[] min; // null => -inf
    public final byte[] max; // null => +inf
    public final boolean minExclusive; // true for strict '>' bounds
    public final boolean maxExclusive; // true for strict '<' bounds

    /** Inclusive range (used for page min/max stats). */
    public ColumnRange(short columnId, byte[] min, byte[] max) {
        this(columnId, min, max, false, false);
    }

    /** Range with explicit bound inclusivity (used for predicate ranges). */
    public ColumnRange(short columnId, byte[] min, byte[] max, boolean minExclusive, boolean maxExclusive) {
        this.columnId = columnId;
        this.min = min;
        this.max = max;
        this.minExclusive = minExclusive;
        this.maxExclusive = maxExclusive;
    }

    /** True if this range and {@code q} could share any value, honoring open/closed bounds. */
    public boolean overlaps(ColumnRange q) {
        // disjoint if this.min > q.max (or equal with an exclusive endpoint)
        if (this.min != null && q.max != null) {
            int c = lex(this.min, q.max);
            if (c > 0 || (c == 0 && (this.minExclusive || q.maxExclusive))) {
                return false;
            }
        }
        // disjoint if q.min > this.max (or equal with an exclusive endpoint)
        if (q.min != null && this.max != null) {
            int c = lex(q.min, this.max);
            if (c > 0 || (c == 0 && (q.minExclusive || this.maxExclusive))) {
                return false;
            }
        }
        return true;
    }

    /** Pure lexicographic unsigned byte comparison — the only operation the bookie needs. */
    public static int lex(byte[] a, byte[] b) {
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
}
