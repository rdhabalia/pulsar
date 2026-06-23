package io.streamlake;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Per-topic, append-only date partition index (the dedicated DateIndexLedger).
 * Maps each day to the ledgers that contain records for that day, enabling the
 * first (cheapest) pruning stage: date range -> candidate ledgers.
 */
public final class DateIndexLedger {
    private final TreeMap<LocalDate, List<LedgerInfo>> partitions = new TreeMap<>();

    /** Append-only: record a closed ledger under its date. */
    public void append(LocalDate date, LedgerInfo info) {
        partitions.computeIfAbsent(date, d -> new ArrayList<>()).add(info);
    }

    /** Stage-1 pruning: ledgers whose date falls within [from, to] inclusive. */
    public List<LedgerInfo> ledgersForDateRange(LocalDate from, LocalDate to) {
        List<LedgerInfo> out = new ArrayList<>();
        for (Map.Entry<LocalDate, List<LedgerInfo>> e : partitions.subMap(from, true, to, true).entrySet()) {
            out.addAll(e.getValue());
        }
        return out;
    }

    public List<LedgerInfo> allLedgers() {
        List<LedgerInfo> out = new ArrayList<>();
        for (List<LedgerInfo> v : partitions.values()) {
            out.addAll(v);
        }
        return out;
    }

    public java.util.Set<LocalDate> dates() {
        return partitions.keySet();
    }

    public int partitionCount() {
        return partitions.size();
    }
}
