package io.streamlake;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * End-to-end Streaming Lake demo (runs on the reference engine, i.e. the broker + bookie
 * model). It:
 *
 *   1. starts the engine (bookie + broker),
 *   2. creates a "persons" topic (departmentId, salary indexed),
 *   3. has a PRODUCER publish N (configurable) Person records,
 *   4. a PUB-SUB consumer consumes all messages (decoding columnar pages back to rows),
 *   5. a full-scan query consumer writes ALL records to a file (SELECT * FROM persons),
 *   6. a filtered query consumer writes the rows matching
 *        departmentId > X AND departmentId < Y AND salary > Z
 *      to a second file (X, Y, Z configurable).
 *
 * Usage: StreamingLakeClusterDemo [numRecords] [deptX] [deptY] [salaryZ] [outDir]
 * Defaults: 1000 5 15 100 demo-output
 */
public final class StreamingLakeClusterDemo {

    static final short NAME = 1;
    static final short DEPT = 2;
    static final short SALARY = 3;
    static final long DAY = 86_400_000L;

    public static void main(String[] args) throws IOException {
        int numRecords = args.length > 0 ? Integer.parseInt(args[0]) : 1000;
        int deptX = args.length > 1 ? Integer.parseInt(args[1]) : 5;
        int deptY = args.length > 2 ? Integer.parseInt(args[2]) : 15;
        int salaryZ = args.length > 3 ? Integer.parseInt(args[3]) : 100;
        int startDay = args.length > 4 ? Integer.parseInt(args[4]) : 0;   // date-partition range start (day offset)
        int endDay = args.length > 5 ? Integer.parseInt(args[5]) : 1;     // date-partition range end (day offset)
        Path outDir = Paths.get(args.length > 6 ? args[6] : "demo-output");
        Files.createDirectories(outDir);

        // ---- 1. start engine (bookie + broker) + create topic ----
        SchemaDef schema = new SchemaDef()
                .field(NAME, "name", ColumnType.STRING)
                .field(DEPT, "departmentId", ColumnType.INT)
                .field(SALARY, "salary", ColumnType.INT);
        StreamingLakeConfig config = StreamingLakeConfig.builder()
                .pageSealRecords(8)
                .maxPagesPerLedger(3)
                .indexedColumns(DEPT, SALARY)
                .build();
        Bookie bookie = new Bookie();
        StreamingLakeBroker broker = new StreamingLakeBroker("persons", schema, config, bookie);
        StreamingLakeConsumer consumer = new StreamingLakeConsumer(broker);

        System.out.println("Streaming Lake engine started (bookie + broker). Topic: persons "
                + "[indexed: departmentId, salary]");
        System.out.println("Config: numRecords=" + numRecords
                + ", filter: date_partition in [day+" + startDay + ", day+" + endDay + "]"
                + " AND departmentId > " + deptX
                + " AND departmentId < " + deptY + " AND salary > " + salaryZ + "\n");

        // ---- 2 + 3. PRODUCER publishes numRecords across several days ----
        long day0 = LocalDate.of(2026, 6, 1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        int days = 5;
        int perDay = Math.max(1, (int) Math.ceil(numRecords / (double) days));
        List<Record> produced = new ArrayList<>();
        int g = 0;
        outer:
        for (int d = 0; d < days; d++) {
            long dayStart = day0 + d * DAY;
            for (int i = 0; i < perDay; i++) {
                if (g >= numRecords) {
                    break outer;
                }
                int dept = (g % 20) + 1;                 // 1..20
                int salary = 10 + (g % 30) * 10;         // 10..300
                long eventTime = dayStart + i * 30_000L;
                Record r = new Record(eventTime)
                        .put(NAME, "person-" + g)
                        .put(DEPT, dept)
                        .put(SALARY, salary);
                broker.publish(r);
                produced.add(r);
                g++;
            }
        }
        broker.flush();
        System.out.println("PRODUCER: published " + produced.size() + " records -> "
                + broker.totalPages() + " columnar pages across "
                + broker.dateIndex().allLedgers().size() + " ledgers, "
                + broker.dateIndex().partitionCount() + " date partitions.");

        // ---- 4. PUB-SUB consumer: consume all messages (columnar pages -> rows) ----
        List<Record> consumed = new ArrayList<>();
        for (LedgerInfo li : broker.dateIndex().allLedgers()) {
            int pages = broker.pagesInLedger(li.ledgerId);
            for (int e = 0; e < pages; e++) {
                consumed.addAll(broker.readPageAsRows(li.ledgerId, e));
            }
        }
        System.out.println("PUB-SUB CONSUMER: consumed " + consumed.size()
                + " messages (decoded from columnar pages). e.g. " + consumed.get(0));

        LocalDate fullFrom = StreamingLakeBroker.dateOf(day0);
        LocalDate fullTo = StreamingLakeBroker.dateOf(day0 + (days - 1) * DAY);
        // configurable date-partition range for the filtered query
        LocalDate queryFrom = StreamingLakeBroker.dateOf(day0 + (long) startDay * DAY);
        LocalDate queryTo = StreamingLakeBroker.dateOf(day0 + (long) endDay * DAY);

        // ---- 5. FULL-SCAN query consumer: SELECT * FROM persons (all dates) ----
        Predicate matchAll = Predicate.ge(DEPT, ColumnType.INT, Integer.MIN_VALUE);
        ScanMetrics fullMetrics = new ScanMetrics();
        List<Record> fullScan = consumer.scan(fullFrom, fullTo, matchAll, fullMetrics);
        Path fullFile = outDir.resolve("full_scan.txt");
        writeRecords(fullFile, schema, fullScan,
                "SELECT * FROM persons", fullMetrics);
        System.out.println("\nFULL-SCAN CONSUMER (SELECT *): wrote " + fullScan.size()
                + " records to " + fullFile);

        // ---- 6. FILTERED query consumer: deptId > X AND deptId < Y AND salary > Z ----
        Predicate filter = Predicate.and(
                Predicate.gt(DEPT, ColumnType.INT, deptX),
                Predicate.lt(DEPT, ColumnType.INT, deptY),
                Predicate.gt(SALARY, ColumnType.INT, salaryZ));
        ScanMetrics filterMetrics = new ScanMetrics();
        List<Record> filtered = consumer.scan(queryFrom, queryTo, filter, filterMetrics);
        Path filterFile = outDir.resolve("filtered.txt");
        writeRecords(filterFile, schema, filtered,
                "SELECT * FROM persons WHERE date_partition BETWEEN " + queryFrom + " AND " + queryTo
                        + " AND departmentId > " + deptX + " AND departmentId < " + deptY
                        + " AND salary > " + salaryZ, filterMetrics);
        System.out.println("FILTERED CONSUMER: wrote " + filtered.size()
                + " records to " + filterFile);
        System.out.println("   pruning: read " + filterMetrics.pagesRead + " of "
                + filterMetrics.totalPages + " pages (date pruned " + filterMetrics.pagesPrunedByDate
                + ", range pruned " + filterMetrics.pagesPrunedByRange + ")");

        // ---- sanity check the filtered output ----
        for (Record r : filtered) {
            LocalDate d = StreamingLakeBroker.dateOf(r.eventTime);
            boolean inDateRange = !d.isBefore(queryFrom) && !d.isAfter(queryTo);
            if (!(inDateRange && r.getInt(DEPT) > deptX && r.getInt(DEPT) < deptY
                    && r.getInt(SALARY) > salaryZ)) {
                throw new AssertionError("filtered output contains a non-matching row: " + r);
            }
        }
        System.out.println("\nDone. Full scan: " + fullScan.size() + " rows; filtered: "
                + filtered.size() + " rows. Files in " + outDir.toAbsolutePath());
    }

    private static void writeRecords(Path file, SchemaDef schema, List<Record> records,
                                     String query, ScanMetrics metrics) throws IOException {
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(file))) {
            w.println("# " + query);
            w.println("# matched " + records.size() + " records | " + metrics);
            w.println("# columns: name, departmentId, salary, eventTime");
            for (Record r : records) {
                StringBuilder sb = new StringBuilder();
                for (Field f : schema.fields()) {
                    sb.append(f.name).append('=').append(r.get(f.columnId)).append('\t');
                }
                sb.append("eventTime=").append(r.eventTime);
                w.println(sb.toString());
            }
        }
    }
}
