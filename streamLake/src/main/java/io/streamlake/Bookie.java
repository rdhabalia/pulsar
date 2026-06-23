package io.streamlake;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Models the BookKeeper bookie. Critically, the bookie is <b>schema-agnostic</b>:
 * <ul>
 *   <li>it stores the opaque columnar payload as an entry (digested as-is),</li>
 *   <li>it stores the range sidecar in a separate "page-range index" (the new RocksDB CF),</li>
 *   <li>{@link #giveIndexPages} prunes pages using ONLY {@link ColumnRange#overlaps}
 *       (lexicographic byte comparison) — it never decodes payloads, learns column
 *       types/names, or executes queries.</li>
 * </ul>
 */
public final class Bookie {

    /** Entry log: ledgerId -> ordered entries. */
    private final Map<Long, List<PageEntry>> entryLog = new HashMap<>();
    /** Page-range index (RocksDB CF analog): "ledgerId:entryId" -> ranges. */
    private final Map<String, List<ColumnRange>> pageRangeIndex = new HashMap<>();

    // observable counters for test assertions
    public long entryReadCount = 0;
    public long pruneRequestCount = 0;
    public long byteRangeComparisons = 0;

    private static String key(long ledgerId, long entryId) {
        return ledgerId + ":" + entryId;
    }

    /** Write path: persist the entry and index its range sidecar. */
    public void addEntry(PageEntry entry) {
        entryLog.computeIfAbsent(entry.ledgerId, l -> new ArrayList<>()).add(entry);
        pageRangeIndex.put(key(entry.ledgerId, entry.entryId), entry.ranges);
    }

    /** Read the opaque payload of one page (the broker decodes it). */
    public byte[] readEntry(long ledgerId, long entryId) {
        entryReadCount++;
        for (PageEntry e : entryLog.get(ledgerId)) {
            if (e.entryId == entryId) {
                return e.payload;
            }
        }
        throw new IllegalArgumentException("No such entry " + key(ledgerId, entryId));
    }

    public int entryCount(long ledgerId) {
        List<PageEntry> l = entryLog.get(ledgerId);
        return l == null ? 0 : l.size();
    }

    public boolean hasRangeIndex(long ledgerId, long entryId) {
        return pageRangeIndex.containsKey(key(ledgerId, entryId));
    }

    /**
     * New bookie prune API: iterate the page-range index for entries in
     * [startEntryId, endEntryId] and return the entryIds whose ranges could satisfy
     * the predicate (AND across columns, OR within a column). Pure byte comparison.
     */
    public List<Long> giveIndexPages(long ledgerId, long startEntryId, long endEntryId,
                                     List<Predicate.ColumnPredicateRanges> predicates) {
        pruneRequestCount++;
        List<Long> matches = new ArrayList<>();
        List<PageEntry> entries = entryLog.get(ledgerId);
        if (entries == null) {
            return matches;
        }
        for (PageEntry e : entries) {
            if (e.entryId < startEntryId || e.entryId > endEntryId) {
                continue;
            }
            List<ColumnRange> pageRanges = pageRangeIndex.get(key(ledgerId, e.entryId));
            if (pageCouldMatch(pageRanges, predicates)) {
                matches.add(e.entryId);
            }
        }
        return matches;
    }

    private boolean pageCouldMatch(List<ColumnRange> pageRanges,
                                   List<Predicate.ColumnPredicateRanges> predicates) {
        for (Predicate.ColumnPredicateRanges pred : predicates) {
            ColumnRange pageRange = null;
            for (ColumnRange r : pageRanges) {
                if (r.columnId == pred.columnId) {
                    pageRange = r;
                    break;
                }
            }
            if (pageRange == null) {
                continue; // column not indexed on this page -> cannot prune, keep it
            }
            boolean anyOverlap = false;
            for (ColumnRange q : pred.orRanges) {
                byteRangeComparisons++;
                if (pageRange.overlaps(q)) {
                    anyOverlap = true;
                    break;
                }
            }
            if (!anyOverlap) {
                return false; // this column's constraint excludes the page
            }
        }
        return true;
    }

    /**
     * Recovery (concern ②): rebuild the page-range index purely from the durable,
     * ledger-attached range sidecars stored with each entry. The bookie never needs
     * to parse the columnar payload to recover the index.
     */
    public void recoverPageIndex() {
        pageRangeIndex.clear();
        for (Map.Entry<Long, List<PageEntry>> led : entryLog.entrySet()) {
            for (PageEntry e : led.getValue()) {
                pageRangeIndex.put(key(e.ledgerId, e.entryId), e.ranges);
            }
        }
    }
}
