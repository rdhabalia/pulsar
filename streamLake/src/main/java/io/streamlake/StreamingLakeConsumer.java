package io.streamlake;

import java.time.LocalDate;
import java.util.List;

/**
 * Client-facing Streaming-Lake consumer. {@code scan(...)} drives the broker's
 * date+predicate pruning pipeline and streams back matching records — the
 * {@code StreamingLakeConsumer.scan(dateRange, predicate)} API from the design.
 */
public final class StreamingLakeConsumer {

    private final StreamingLakeBroker broker;

    public StreamingLakeConsumer(StreamingLakeBroker broker) {
        this.broker = broker;
    }

    public List<Record> scan(LocalDate from, LocalDate to, Predicate predicate, ScanMetrics metrics) {
        return broker.scan(from, to, predicate, metrics);
    }

    /** Convenience overload that prints each matching record (the user's ask). */
    public List<Record> scanAndPrint(LocalDate from, LocalDate to, Predicate predicate,
                                     SchemaDef schema, ScanMetrics metrics) {
        List<Record> rows = broker.scan(from, to, predicate, metrics);
        System.out.println("---- scan results (" + rows.size() + " records) ----");
        for (Record r : rows) {
            StringBuilder sb = new StringBuilder();
            for (Field f : schema.fields()) {
                sb.append(f.name).append('=').append(r.get(f.columnId)).append(' ');
            }
            System.out.println("  " + sb.toString().trim());
        }
        return rows;
    }
}
