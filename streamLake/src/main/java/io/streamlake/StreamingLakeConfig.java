package io.streamlake;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-topic Streaming-Lake configuration (mirrors the {@code StreamingLakeConfig}
 * topic policy in the real design). Maps to a new field on Pulsar's TopicPolicies.
 *
 * <p>{@code indexedColumns} are the subset of schema fields for which the broker
 * emits min/max ranges into the page-range index used for pruning. All columns are
 * stored columnar; only indexed ones get a range entry.
 */
public final class StreamingLakeConfig {
    /** Seal a page when it reaches this many records (proxy for the ~2MB size trigger). */
    public final int pageSealRecords;
    /** Roll the ledger after this many sealed pages (forces multiple ledgers). */
    public final int maxPagesPerLedger;
    /** Columns (by id) that get a range entry in the page-range index. */
    public final List<Short> indexedColumns;

    private StreamingLakeConfig(Builder b) {
        this.pageSealRecords = b.pageSealRecords;
        this.maxPagesPerLedger = b.maxPagesPerLedger;
        this.indexedColumns = b.indexedColumns;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private int pageSealRecords = 1000;
        private int maxPagesPerLedger = 100;
        private final List<Short> indexedColumns = new ArrayList<>();

        public Builder pageSealRecords(int v) {
            this.pageSealRecords = v;
            return this;
        }

        public Builder maxPagesPerLedger(int v) {
            this.maxPagesPerLedger = v;
            return this;
        }

        public Builder indexedColumns(short... ids) {
            for (short id : ids) {
                indexedColumns.add(id);
            }
            return this;
        }

        public StreamingLakeConfig build() {
            return new StreamingLakeConfig(this);
        }
    }
}
