package io.streamlake;

/** Per-scan observability used to prove pruning actually happened. */
public final class ScanMetrics {
    public int totalPages;
    public int candidateLedgers;
    public int pagesAfterDatePruning;   // pages in candidate ledgers
    public int pagesPrunedByDate;       // totalPages - pagesAfterDatePruning
    public int pagesAfterRangePruning;  // pages the bookie returned as candidates
    public int pagesPrunedByRange;      // pagesAfterDatePruning - pagesAfterRangePruning
    public int pagesRead;               // pages actually fetched + decoded by the broker
    public int rowsScanned;             // rows the codec looked at within read pages
    public int rowsReturned;            // rows that passed exact predicate

    @Override
    public String toString() {
        return "ScanMetrics{totalPages=" + totalPages
                + ", prunedByDate=" + pagesPrunedByDate
                + ", prunedByRange=" + pagesPrunedByRange
                + ", pagesRead=" + pagesRead
                + ", rowsReturned=" + rowsReturned + "}";
    }
}
