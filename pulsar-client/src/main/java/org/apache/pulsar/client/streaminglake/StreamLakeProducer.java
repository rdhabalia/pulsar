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
 * elapsed since the oldest buffered row (a background flusher, so low-rate producers stay fresh). The
 * wrapped producer should be created with Pulsar batching disabled and message compression enabled.
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
        return producer.sendAsync(payload).thenAccept(id -> { });
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
