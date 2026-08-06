/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.streaminglake;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accumulates rows and writes them as StreamLake columnar batches over a raw {@code Producer<byte[]>}:
 * on flush it Arrow-encodes the buffered rows, computes the per-batch pruning stats footer, frames
 * them into one payload ({@link StreamLakeBatchPayload}) and sends it as a single Pulsar message
 * (one message = one columnar batch = one BookKeeper entry).
 *
 * <p>Flush triggers: {@code maxRows} reached, estimated {@code maxBytes} reached, or {@code maxDelayMs}
 * elapsed since the oldest buffered row (a background flusher, so low-rate producers stay fresh).
 *
 * <p><b>The wrapped producer MUST be created with batching disabled and message compression set to
 * {@code NONE}.</b> StreamLake compresses the Arrow region itself inside {@link StreamLakeBatchPayload}
 * and leaves the stats footer + trailer in the clear so the broker can slice the footer off the entry
 * tail without decoding. If Pulsar message compression were on, it would compress the whole payload and
 * bury that trailing footer marker, so the broker would never index the page and the data would be
 * unqueryable (0 candidate ledgers at query time).
 *
 * <p>Thread-safe; {@link #close()} flushes the remainder and releases the Arrow allocator. It does not
 * close the wrapped producer (the caller owns it).
 */
public final class StreamLakeProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeProducer.class);

    private final Producer<byte[]> producer;
    private final StreamLakeTopicSchema topicSchema;
    private final StreamLakeArrowBatchEncoder encoder;
    private final int maxRows;
    private final long maxBytes;
    private final long maxDelayMs;
    private final ScheduledExecutorService flusher;

    private final List<Object[]> buffer = new ArrayList<>();
    private long bufferedBytes;
    private long oldestRowNanos;

    // Send-failure accounting so callers can verify completeness: a nonzero count means one or more
    // page sends never persisted, so rows are missing (do not trust the attempted-row count alone).
    private final AtomicLong sendFailures = new AtomicLong();
    private volatile Throwable lastSendFailure;

    public StreamLakeProducer(Producer<byte[]> producer, StreamLakeTopicSchema topicSchema) {
        this(producer, topicSchema, 1000, 1024 * 1024, 10);
    }

    public StreamLakeProducer(Producer<byte[]> producer, StreamLakeTopicSchema topicSchema,
            int maxRows, long maxBytes, long maxDelayMs) {
        this.producer = producer;
        this.topicSchema = topicSchema;
        this.encoder = new StreamLakeArrowBatchEncoder(topicSchema.schema());
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
        this.maxDelayMs = maxDelayMs;
        if (maxDelayMs > 0) {
            this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "streamlake-flush");
                t.setDaemon(true);
                return t;
            });
            this.flusher.scheduleWithFixedDelay(this::flushIfStaleQuietly, maxDelayMs, maxDelayMs,
                    TimeUnit.MILLISECONDS);
        } else {
            this.flusher = null;
        }
    }

    /** Buffer a row (aligned to the topic schema columns); flushes if a size threshold is reached. */
    public synchronized CompletableFuture<Void> addRow(Object[] row) {
        if (buffer.isEmpty()) {
            oldestRowNanos = System.nanoTime();
        }
        buffer.add(row);
        bufferedBytes += estimate(row);
        if (buffer.size() >= maxRows || bufferedBytes >= maxBytes) {
            return doFlush();
        }
        return CompletableFuture.completedFuture(null);
    }

    /** Flush the buffered rows now as one columnar message (no-op if empty). */
    public synchronized CompletableFuture<Void> flush() {
        return buffer.isEmpty() ? CompletableFuture.completedFuture(null) : doFlush();
    }

    private CompletableFuture<Void> doFlush() {
        List<Object[]> rows = new ArrayList<>(buffer);
        buffer.clear();
        bufferedBytes = 0;
        byte[] arrow = encoder.encode(rows);
        byte[] footer = StreamLakeStatsBuilder.build(topicSchema.schema(), rows,
                topicSchema.indexedColumns(), topicSchema.setMaxCardinality(), topicSchema.bloomFpp())
                .encode();
        byte[] payload = StreamLakeBatchPayload.combine(arrow, footer);
        CompletableFuture<Void> f = producer.sendAsync(payload).thenAccept(id -> { });
        // Record (do not swallow) send failures so callers can detect missing data. whenComplete leaves
        // the returned future's outcome unchanged for any caller that also inspects it.
        f.whenComplete((v, ex) -> {
            if (ex != null) {
                sendFailures.incrementAndGet();
                lastSendFailure = ex;
            }
        });
        return f;
    }

    /**
     * Number of page sends that failed to persist. A nonzero value means the corresponding rows are
     * missing from the topic — the data is incomplete regardless of how many rows were buffered. Final
     * only after the wrapped producer has flushed all in-flight sends (e.g. {@code producer.flush()}).
     */
    public long sendFailures() {
        return sendFailures.get();
    }

    /** The most recent send failure (for diagnostics), or {@code null} if every send succeeded. */
    public Throwable lastSendFailure() {
        return lastSendFailure;
    }

    private void flushIfStaleQuietly() {
        try {
            synchronized (this) {
                if (!buffer.isEmpty()
                        && System.nanoTime() - oldestRowNanos >= maxDelayMs * 1_000_000L) {
                    doFlush();
                }
            }
        } catch (RuntimeException e) {
            log.warn("StreamLake time-based flush failed: {}", e.toString());
        }
    }

    private static long estimate(Object[] row) {
        long bytes = 0;
        for (Object v : row) {
            if (v == null) {
                continue;
            } else if (v instanceof String) {
                bytes += ((String) v).length() * 2L + 4;
            } else if (v instanceof byte[]) {
                bytes += ((byte[]) v).length + 4L;
            } else if (v instanceof Integer) {
                bytes += 4;
            } else {
                bytes += 8;
            }
        }
        return bytes;
    }

    @Override
    public synchronized void close() {
        try {
            if (!buffer.isEmpty()) {
                doFlush();
            }
        } finally {
            if (flusher != null) {
                flusher.shutdownNow();
            }
            encoder.close();
        }
    }
}
