package io.streamlake;

import java.time.LocalDate;

/** Metadata recorded in the DateIndexLedger when a ledger is closed. */
public final class LedgerInfo {
    public final long ledgerId;
    public final long startTimestamp;
    public final long endTimestamp;
    public final LocalDate date;

    public LedgerInfo(long ledgerId, long startTimestamp, long endTimestamp, LocalDate date) {
        this.ledgerId = ledgerId;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.date = date;
    }

    @Override
    public String toString() {
        return "Ledger" + ledgerId + "[" + date + "]";
    }
}
