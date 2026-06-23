package io.streamlake;

import java.util.List;

/**
 * Pluggable page codec. Per our design decision, the columnar format is abstracted
 * behind this interface so a Vortex (JNI) implementation can be swapped in later
 * without touching the broker/bookie; {@link ColumnarPageCodec} is the JVM-native
 * reference implementation (the "Arrow/Parquet fallback" we agreed to start with).
 */
public interface PageCodec {

    /** Reserved columnId used to carry the per-row eventTime inside the page. */
    short EVENT_TIME_COLUMN = 0;

    /** Encode a batch of rows into a self-describing columnar page. */
    byte[] encode(List<Record> rows);

    /** Full reconstruction back to rows — the pub-sub read path. */
    List<Record> decodeAll(byte[] page);

    /**
     * Predicate pushdown: evaluate {@code predicate} per row against the encoded
     * columns and materialize ONLY the matching rows.
     */
    List<Record> decodeMatching(byte[] page, Predicate predicate);

    /** Compute order-preserving min/max ranges for the indexed columns (the opaque blob). */
    List<ColumnRange> computeRanges(List<Record> rows, List<Short> indexedColumns);

    /** Min/max eventTime across the page (drives date partitioning). */
    long[] eventTimeMinMax(List<Record> rows);
}
