package io.streamlake;

/**
 * Typed access to a single row's columns by columnId. Implemented both by
 * {@link Record} (a fully materialized row) and by the columnar codec's row view
 * (random access into encoded bytes, used for predicate pushdown without
 * materializing every row).
 */
public interface RowAccessor {
    Object get(short columnId);

    int getInt(short columnId);

    long getLong(short columnId);

    String getString(short columnId);
}
