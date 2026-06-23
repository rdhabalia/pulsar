package io.streamlake;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Demonstrates the exact flow the question asks about:
 *
 *   1. a producer writes Person records to a topic,
 *   2. records are date-partitioned across multiple ledgers/pages,
 *   3. a query consumer runs:
 *
 *        SELECT * FROM persons
 *        WHERE date_partition >= startRange
 *          AND date_partition <= endRange
 *          AND deptId > 10
 *
 * and the matching records are returned and validated against a brute-force oracle.
 *
 * <p>This runs against the Streaming Lake reference engine (the same broker/bookie/page
 * model as StreamingLakeEndToEndTest). Run with: javac + java, see streamLake/run.sh.
 */
public final class StreamingLakeQueryDemo {

    static final short NAME = 1;
    static final short DEPT = 2;
    static final short SALARY = 3;
    static final long DAY = 86_400_000L;

    public static void main(String[] args) {
        SchemaDef schema = new SchemaDef()
                .field(NAME, "name", ColumnType.STRING)
                .field(DEPT, "departmentId", ColumnType.INT)
                .field(SALARY, "salary", ColumnType.INT);

        // small pages/ledgers so we really get date partitions, multiple ledgers, multiple pages
        StreamingLakeConfig config = StreamingLakeConfig.builder()
                .pageSealRecords(8)
                .maxPagesPerLedger(3)
                .indexedColumns(DEPT, SALARY)
                .build();

        Bookie bookie = new Bookie();
        StreamingLakeBroker broker = new StreamingLakeBroker("persons", schema, config, bookie);
        StreamingLakeConsumer consumer = new StreamingLakeConsumer(broker);

        long day0 = LocalDate.of(2026, 6, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

        // ---- 1. producer writes records across 3 days, deptId 1..20 ----
        List<Record> all = new ArrayList<>();
        int g = 0;
        for (int d = 0; d < 3; d++) {
            long dayStart = day0 + d * DAY;
            for (int i = 0; i < 90; i++) {
                int dept = (i % 20) + 1;              // 1..20
                int salary = 30 + (i % 25) * 8;       // varied
                long eventTime = dayStart + i * 60_000L;
                Record r = new Record(eventTime)
                        .put(NAME, "p" + g)
                        .put(DEPT, dept)
                        .put(SALARY, salary);
                broker.publish(r);
                all.add(r);
                g++;
            }
        }
        broker.flush();
        System.out.println("Produced " + all.size() + " records -> "
                + broker.totalPages() + " pages across "
                + broker.dateIndex().allLedgers().size() + " ledgers, "
                + broker.dateIndex().partitionCount() + " date partitions.\n");

        // ---- 2 + 3. query consumer: date_partition in [day0, day1] AND deptId > 10 ----
        LocalDate startRange = StreamingLakeBroker.dateOf(day0);             // 2026-06-01
        LocalDate endRange = StreamingLakeBroker.dateOf(day0 + DAY);         // 2026-06-02  (excludes day 3)
        Predicate query = Predicate.gt(DEPT, ColumnType.INT, 10);

        System.out.println("Query: SELECT * FROM persons "
                + "WHERE date_partition BETWEEN " + startRange + " AND " + endRange
                + " AND departmentId > 10\n");

        ScanMetrics metrics = new ScanMetrics();
        List<Record> result = consumer.scanAndPrint(startRange, endRange, query, schema, metrics);
        System.out.println("\n" + metrics);

        // ---- validate against a brute-force oracle ----
        List<String> oracle = new ArrayList<>();
        for (Record r : all) {
            LocalDate d = StreamingLakeBroker.dateOf(r.eventTime);
            if (!d.isBefore(startRange) && !d.isAfter(endRange) && r.getInt(DEPT) > 10) {
                oracle.add(canonical(r));
            }
        }
        List<String> got = new ArrayList<>();
        for (Record r : result) {
            got.add(canonical(r));
            if (!(r.getInt(DEPT) > 10)) {
                throw new AssertionError("returned a row with deptId <= 10: " + r);
            }
        }
        java.util.Collections.sort(oracle);
        java.util.Collections.sort(got);
        if (!oracle.equals(got)) {
            throw new AssertionError("scan result != oracle (got " + got.size()
                    + ", expected " + oracle.size() + ")");
        }
        if (metrics.pagesPrunedByDate <= 0 || metrics.pagesPrunedByRange <= 0) {
            throw new AssertionError("expected both date and range pruning to drop pages: " + metrics);
        }
        System.out.println("\nVALIDATED: " + result.size() + " records match the oracle; "
                + "read only " + metrics.pagesRead + " of " + metrics.totalPages + " pages "
                + "(date pruned " + metrics.pagesPrunedByDate + ", range pruned "
                + metrics.pagesPrunedByRange + "). ✅");
    }

    static String canonical(Record r) {
        return r.getString(NAME) + "|" + r.getInt(DEPT) + "|" + r.getInt(SALARY) + "|" + r.eventTime;
    }
}
