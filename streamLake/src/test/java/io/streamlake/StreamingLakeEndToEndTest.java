package io.streamlake;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end test for the Streaming Lake engine.
 *
 * <p>Scenario: a {@code persons} topic (name, departmentId, salary) is filled with
 * enough records across 3 days to create a date-partitioned index, multiple ledgers
 * per day, and multiple columnar pages per ledger. A consumer then scans a 2-day
 * window for {@code departmentId > 1 AND (salary < 50 OR salary > 100)} and we assert
 * the full pipeline: date pruning, bookie range pruning, page reads, row-level
 * pushdown, correctness vs. a brute-force oracle, pub-sub decode, and index recovery.
 *
 * <p>Self-contained runner (no test framework needed): {@code main} runs every check
 * and exits non-zero on the first failure.
 */
public final class StreamingLakeEndToEndTest {

    // schema columnIds
    static final short NAME = 1;
    static final short DEPT = 2;
    static final short SALARY = 3;

    static final int PAGE = 8;            // records per page (size trigger proxy)
    static final int MAX_PAGES_PER_LEDGER = 3;
    static final int N_PER_DATE = 80;     // 10 pages/date -> 4 ledgers/date
    static final int[] SAL = {30, 60, 90, 120, 200};

    // shared state built once
    static SchemaDef schema;
    static StreamingLakeBroker broker;
    static Bookie bookie;
    static StreamingLakeConsumer consumer;
    static List<Record> allRecords;
    static long day0;
    static final long DAY = 86_400_000L;

    public static void main(String[] args) {
        int failures = 0;
        try {
            setUp();
            run("date-partitioned ledger created", StreamingLakeEndToEndTest::testDatePartitioning);
            run("multiple ledgers with multiple pages", StreamingLakeEndToEndTest::testMultipleLedgersAndPages);
            run("pub-sub columnar decode round-trip", StreamingLakeEndToEndTest::testPubSubDecode);
            run("end-to-end pruned scan (deptId>1 AND (salary<50 OR salary>100))",
                    StreamingLakeEndToEndTest::testRangePrunedScan);
            run("page-index recovery rebuilds from sidecars", StreamingLakeEndToEndTest::testRecovery);
        } catch (Throwable t) {
            failures++;
            System.out.println("FATAL during setup: " + t);
            t.printStackTrace();
        }
        if (FAILED > 0) {
            failures += FAILED;
        }
        System.out.println();
        if (failures == 0) {
            System.out.println("ALL TESTS PASSED ✅");
        } else {
            System.out.println(failures + " CHECK(S) FAILED ❌");
            System.exit(1);
        }
    }

    // ------------------------------------------------------------------ setup

    static void setUp() {
        schema = new SchemaDef()
                .field(NAME, "name", ColumnType.STRING)
                .field(DEPT, "departmentId", ColumnType.INT)
                .field(SALARY, "salary", ColumnType.INT);

        StreamingLakeConfig config = StreamingLakeConfig.builder()
                .pageSealRecords(PAGE)
                .maxPagesPerLedger(MAX_PAGES_PER_LEDGER)
                .indexedColumns(DEPT, SALARY)
                .build();

        bookie = new Bookie();
        broker = new StreamingLakeBroker("persons", schema, config, bookie);
        consumer = new StreamingLakeConsumer(broker);
        allRecords = new ArrayList<>();

        day0 = LocalDate.of(2026, 6, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

        int g = 0;
        for (int d = 0; d < 3; d++) {
            long dayStart = day0 + d * DAY;
            for (int i = 0; i < N_PER_DATE; i++) {
                int page = i / PAGE;
                int dept = (page % 3) + 1;
                int salary = SAL[page % SAL.length] + (i % PAGE);
                long eventTime = dayStart + i * 60_000L;
                Record r = new Record(eventTime)
                        .put(NAME, "p" + g)
                        .put(DEPT, dept)
                        .put(SALARY, salary);
                broker.publish(r);
                allRecords.add(r);
                g++;
            }
        }
        broker.flush();
        System.out.println("Setup: published " + allRecords.size() + " records, "
                + broker.totalPages() + " pages, "
                + broker.dateIndex().allLedgers().size() + " ledgers across "
                + broker.dateIndex().partitionCount() + " date partitions.\n");
    }

    // ------------------------------------------------------------------ tests

    static void testDatePartitioning() {
        assertEquals(3, broker.dateIndex().partitionCount(), "date partitions");
        // each of the 3 days must map to ledgers
        for (LocalDate d : broker.dateIndex().dates()) {
            assertTrue(!broker.dateIndex().ledgersForDateRange(d, d).isEmpty(),
                    "ledgers exist for " + d);
        }
        assertEquals(240, allRecords.size(), "total records");
        assertEquals(30, broker.totalPages(), "total pages (240/8)");
    }

    static void testMultipleLedgersAndPages() {
        List<LedgerInfo> ledgers = broker.dateIndex().allLedgers();
        assertEquals(12, ledgers.size(), "total ledgers (4 per day x 3 days)");

        int maxPages = 0;
        int multiPageLedgers = 0;
        for (LedgerInfo li : ledgers) {
            int pages = broker.pagesInLedger(li.ledgerId);
            assertTrue(pages >= 1 && pages <= MAX_PAGES_PER_LEDGER, "ledger page count in bounds");
            maxPages = Math.max(maxPages, pages);
            if (pages > 1) {
                multiPageLedgers++;
            }
        }
        assertEquals(MAX_PAGES_PER_LEDGER, maxPages, "some ledger packs the max pages");
        assertTrue(multiPageLedgers >= 6, "many ledgers hold multiple pages");
    }

    static void testPubSubDecode() {
        LedgerInfo li = broker.dateIndex().allLedgers().get(0);
        List<Record> rows = broker.readPageAsRows(li.ledgerId, 0);
        assertEquals(PAGE, rows.size(), "page decodes back to its rows");

        // round-trip integrity: types + values survive columnar encode/decode
        for (Record r : rows) {
            assertTrue(r.getString(NAME).startsWith("p"), "name round-trips");
            assertTrue(r.getInt(DEPT) >= 1 && r.getInt(DEPT) <= 3, "dept round-trips");
            assertTrue(r.getInt(SALARY) > 0, "salary round-trips");
        }

        // pushdown consistency: decodeMatching == filter(decodeAll)
        Predicate p = buildPredicate();
        byte[] pageBytes = bookie.readEntry(li.ledgerId, 0);
        List<Record> pushed = new ColumnarPageCodec(schema).decodeMatching(pageBytes, p);
        int expected = 0;
        for (Record r : rows) {
            if (p.eval(r)) {
                expected++;
            }
        }
        assertEquals(expected, pushed.size(), "pushdown matches full-scan filter");
    }

    static void testRangePrunedScan() {
        LocalDate from = dateOf(0);
        LocalDate to = dateOf(1);   // exclude day 2 -> proves date pruning
        Predicate predicate = buildPredicate();

        ScanMetrics m = new ScanMetrics();
        List<Record> result = consumer.scanAndPrint(from, to, predicate, schema, m);
        System.out.println("  " + m + " | bookie byteRangeComparisons=" + bookie.byteRangeComparisons);

        // --- pruning actually happened ---
        assertEquals(30, m.totalPages, "total pages seen");
        assertEquals(20, m.pagesAfterDatePruning, "pages after date pruning (2 of 3 days)");
        assertEquals(10, m.pagesPrunedByDate, "pages pruned by date");
        assertTrue(m.pagesPrunedByDate > 0, "date pruning removed pages");
        assertEquals(6, m.pagesAfterRangePruning, "pages surviving range pruning");
        assertEquals(14, m.pagesPrunedByRange, "pages pruned by range");
        assertTrue(m.pagesPrunedByRange > 0, "range pruning removed pages");
        assertEquals(6, m.pagesRead, "only candidate pages were read");
        assertTrue(m.pagesRead < m.totalPages, "read far fewer than total pages");

        // --- correctness vs brute-force oracle ---
        List<String> oracle = new ArrayList<>();
        for (Record r : allRecords) {
            LocalDate d = StreamingLakeBroker.dateOf(r.eventTime);
            if (!d.isBefore(from) && !d.isAfter(to) && predicate.eval(r)) {
                oracle.add(canonical(r));
            }
        }
        List<String> got = new ArrayList<>();
        for (Record r : result) {
            got.add(canonical(r));
            // every returned row must truly satisfy the predicate
            assertTrue(r.getInt(DEPT) > 1 && (r.getInt(SALARY) < 50 || r.getInt(SALARY) > 100),
                    "returned row satisfies predicate: " + r);
        }
        Collections.sort(oracle);
        Collections.sort(got);
        assertEquals(oracle.size(), got.size(), "result count == oracle count");
        assertTrue(oracle.equals(got), "scan result exactly equals brute-force oracle");
        assertEquals(48, result.size(), "expected matching record count");
    }

    static void testRecovery() {
        Predicate predicate = buildPredicate();
        ScanMetrics before = new ScanMetrics();
        List<Record> r1 = broker.scan(dateOf(0), dateOf(1), predicate, before);

        // simulate bookie restart: wipe and rebuild the page-range index from sidecars
        bookie.recoverPageIndex();
        assertTrue(bookie.hasRangeIndex(broker.dateIndex().allLedgers().get(0).ledgerId, 0),
                "index repopulated after recovery");

        ScanMetrics after = new ScanMetrics();
        List<Record> r2 = broker.scan(dateOf(0), dateOf(1), predicate, after);

        assertEquals(r1.size(), r2.size(), "same result size after recovery");
        assertEquals(before.pagesPrunedByRange, after.pagesPrunedByRange, "same pruning after recovery");

        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        for (Record r : r1) {
            a.add(canonical(r));
        }
        for (Record r : r2) {
            b.add(canonical(r));
        }
        Collections.sort(a);
        Collections.sort(b);
        assertTrue(a.equals(b), "identical results before/after recovery");
    }

    // ------------------------------------------------------------------ helpers

    static Predicate buildPredicate() {
        // departmentId > 1 AND (salary < 50 OR salary > 100)
        return Predicate.and(
                Predicate.gt(DEPT, ColumnType.INT, 1),
                Predicate.or(
                        Predicate.lt(SALARY, ColumnType.INT, 50),
                        Predicate.gt(SALARY, ColumnType.INT, 100)));
    }

    static LocalDate dateOf(int dayOffset) {
        return StreamingLakeBroker.dateOf(day0 + dayOffset * DAY);
    }

    static String canonical(Record r) {
        return r.getString(NAME) + "|" + r.getInt(DEPT) + "|" + r.getInt(SALARY) + "|" + r.eventTime;
    }

    // ---- tiny assertion harness ----
    static int FAILED = 0;

    interface Check {
        void run();
    }

    static void run(String name, Check c) {
        try {
            c.run();
            System.out.println("PASS  " + name);
        } catch (Throwable t) {
            FAILED++;
            System.out.println("FAIL  " + name + "  ->  " + t.getMessage());
        }
    }

    static void assertTrue(boolean cond, String msg) {
        if (!cond) {
            throw new AssertionError("expected true: " + msg);
        }
    }

    static void assertEquals(Object expected, Object actual, String msg) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new AssertionError(msg + " (expected=" + expected + ", actual=" + actual + ")");
        }
    }
}
