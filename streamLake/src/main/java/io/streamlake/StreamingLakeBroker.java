package io.streamlake;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Models the Pulsar broker for a single Streaming-Lake topic. Responsibilities
 * (mirroring the real broker write/read paths):
 * <ul>
 *   <li><b>Write:</b> buffer records per topic, seal a columnar page at the size
 *       trigger, write it as one BK entry (1 page = 1 entry), roll the ledger on a
 *       date boundary or after N pages, and append the closed ledger to the
 *       DateIndexLedger.</li>
 *   <li><b>Read (scan):</b> date-prune -> bookie range-prune -> read candidate
 *       pages -> Vortex/columnar row-level pushdown -> stream matching records.</li>
 *   <li><b>Pub-sub read:</b> decode a page back into ordered rows.</li>
 * </ul>
 */
public final class StreamingLakeBroker {

    private final String topic;
    private final SchemaDef schema;
    private final StreamingLakeConfig config;
    private final PageCodec codec;
    private final Bookie bookie;
    private final DateIndexLedger dateIndex = new DateIndexLedger();

    // write-path state
    private final List<Record> buffer = new ArrayList<>();
    private long nextLedgerId = 100;
    private long currentLedgerId = -1;
    private LocalDate currentLedgerDate;
    private long currentEntryId = 0;
    private int pagesInCurrentLedger = 0;
    private long currentLedgerStartTs = Long.MAX_VALUE;
    private long currentLedgerEndTs = Long.MIN_VALUE;

    // bookkeeping for tests/metrics
    private final Map<Long, Integer> ledgerPageCount = new HashMap<>();
    private int totalPages = 0;

    public StreamingLakeBroker(String topic, SchemaDef schema, StreamingLakeConfig config, Bookie bookie) {
        this.topic = topic;
        this.schema = schema;
        this.config = config;
        this.bookie = bookie;
        this.codec = new ColumnarPageCodec(schema);
    }

    public static LocalDate dateOf(long eventTimeMillis) {
        return Instant.ofEpochMilli(eventTimeMillis).atZone(ZoneOffset.UTC).toLocalDate();
    }

    // ------------------------------------------------------------------ write

    public void publish(Record r) {
        LocalDate date = dateOf(r.eventTime);
        if (currentLedgerId == -1 || !date.equals(currentLedgerDate)) {
            sealPage();            // seal any pending page in the old ledger
            closeCurrentLedger();  // roll on date boundary
            openNewLedger(date);
        }
        buffer.add(r);
        currentLedgerStartTs = Math.min(currentLedgerStartTs, r.eventTime);
        currentLedgerEndTs = Math.max(currentLedgerEndTs, r.eventTime);

        if (buffer.size() >= config.pageSealRecords) {
            sealPage();
            if (pagesInCurrentLedger >= config.maxPagesPerLedger) {
                closeCurrentLedger(); // roll after N pages -> multiple ledgers per date
            }
        }
    }

    /** Flush remaining buffered records and close the open ledger. */
    public void flush() {
        sealPage();
        closeCurrentLedger();
    }

    private void openNewLedger(LocalDate date) {
        currentLedgerId = nextLedgerId++;
        currentLedgerDate = date;
        currentEntryId = 0;
        pagesInCurrentLedger = 0;
        currentLedgerStartTs = Long.MAX_VALUE;
        currentLedgerEndTs = Long.MIN_VALUE;
    }

    private void sealPage() {
        if (buffer.isEmpty()) {
            return;
        }
        byte[] payload = codec.encode(buffer);
        List<ColumnRange> ranges = codec.computeRanges(buffer, config.indexedColumns);
        long[] et = codec.eventTimeMinMax(buffer);
        long entryId = currentEntryId++;
        bookie.addEntry(new PageEntry(currentLedgerId, entryId, payload, ranges,
                buffer.size(), et[0], et[1]));
        ledgerPageCount.merge(currentLedgerId, 1, Integer::sum);
        pagesInCurrentLedger++;
        totalPages++;
        buffer.clear();
    }

    private void closeCurrentLedger() {
        if (currentLedgerId == -1) {
            return;
        }
        if (pagesInCurrentLedger > 0) {
            dateIndex.append(currentLedgerDate,
                    new LedgerInfo(currentLedgerId, currentLedgerStartTs, currentLedgerEndTs, currentLedgerDate));
        }
        currentLedgerId = -1;
    }

    // ------------------------------------------------------------------ scan

    /**
     * Lakehouse scan: date range + predicate. Returns matching records and fills
     * {@code metrics} so callers can assert that pruning actually occurred.
     */
    public List<Record> scan(LocalDate from, LocalDate to, Predicate predicate, ScanMetrics metrics) {
        metrics.totalPages = totalPages;

        // Stage 1: date pruning
        List<LedgerInfo> candidates = dateIndex.ledgersForDateRange(from, to);
        metrics.candidateLedgers = candidates.size();
        int pagesInCandidates = 0;
        for (LedgerInfo li : candidates) {
            pagesInCandidates += ledgerPageCount.getOrDefault(li.ledgerId, 0);
        }
        metrics.pagesAfterDatePruning = pagesInCandidates;
        metrics.pagesPrunedByDate = totalPages - pagesInCandidates;

        // Stage 2: bookie range pruning (schema-agnostic, byte comparison)
        List<Predicate.ColumnPredicateRanges> preds = predicate.extractColumnPredicates();
        List<Record> result = new ArrayList<>();
        int matchedPages = 0;
        for (LedgerInfo li : candidates) {
            int lastEntry = ledgerPageCount.getOrDefault(li.ledgerId, 0) - 1;
            List<Long> matchedEntryIds = bookie.giveIndexPages(li.ledgerId, 0, lastEntry, preds);
            matchedPages += matchedEntryIds.size();

            // Stage 3 + 4: read candidate pages, row-level pushdown
            for (long entryId : matchedEntryIds) {
                byte[] payload = bookie.readEntry(li.ledgerId, entryId);
                metrics.pagesRead++;
                List<Record> rows = codec.decodeMatching(payload, predicate);
                metrics.rowsReturned += rows.size();
                result.addAll(rows);
            }
        }
        metrics.pagesAfterRangePruning = matchedPages;
        metrics.pagesPrunedByRange = pagesInCandidates - matchedPages;
        return result;
    }

    // ------------------------------------------------------------------ pub-sub

    /** Pub-sub read path: decode a page back into ordered rows (item 6). */
    public List<Record> readPageAsRows(long ledgerId, long entryId) {
        return codec.decodeAll(bookie.readEntry(ledgerId, entryId));
    }

    // ------------------------------------------------------------------ accessors for tests

    public DateIndexLedger dateIndex() {
        return dateIndex;
    }

    public int totalPages() {
        return totalPages;
    }

    public int pagesInLedger(long ledgerId) {
        return ledgerPageCount.getOrDefault(ledgerId, 0);
    }

    public String topic() {
        return topic;
    }
}
