package io.streamlake;

import java.util.List;

/**
 * One sealed page == one BookKeeper entry (the simplification: 1 page = 1 entry,
 * key {@code (ledgerId, entryId)}). Holds the opaque columnar payload plus the
 * durable, ledger-attached range sidecar so the page index is always recoverable
 * (concern ②) even though the bookie cannot parse the payload.
 */
public final class PageEntry {
    public final long ledgerId;
    public final long entryId;
    public final byte[] payload;          // opaque columnar bytes (digested as-is)
    public final List<ColumnRange> ranges; // sidecar range blob (recoverable index source)
    public final int rowCount;
    public final long minEventTime;
    public final long maxEventTime;

    public PageEntry(long ledgerId, long entryId, byte[] payload, List<ColumnRange> ranges,
                     int rowCount, long minEventTime, long maxEventTime) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.payload = payload;
        this.ranges = ranges;
        this.rowCount = rowCount;
        this.minEventTime = minEventTime;
        this.maxEventTime = maxEventTime;
    }
}
