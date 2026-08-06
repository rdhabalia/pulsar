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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.streaminglake.StreamLakeProducer;
import org.apache.pulsar.client.streaminglake.StreamLakeSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeTopicSchema;
import org.apache.pulsar.client.streaminglake.StreamLakeType;

/**
 * Standalone StreamLake load generator for the demo. Writes client-encoded columnar rows to a
 * registered {@code Person} or {@code Employee} table until a target on-disk size (GB) or a fixed row
 * count is reached, printing throughput as it goes. Compiled + run on the host against the Pulsar
 * distribution's {@code lib/*} (see {@code sl-ingest.sh}); it uses only the public client + admin API.
 *
 * <p>The generated data is intentionally checkable: row {@code i} has {@code Person.age = 20 + i%50}
 * and {@code Employee.salary = 30000 + (i%50)*1000}, with {@code personId = i} shared by both tables —
 * so the demo join {@code age in [30,40] AND salary >= 40000} qualifies exactly {@code i%50 in [10,20]}.
 *
 * <pre>
 * java -cp "lib/*:." StreamLakeIngest \
 *   --service-url pulsar://localhost:6650 --admin-url http://localhost:8080 \
 *   --tenant public --namespace default --table Person \
 *   --target-gb 500 --rows-per-page 1000 --start-id 0
 * </pre>
 * Use {@code --rows N} instead of {@code --target-gb} to write an exact row count.
 */
public final class StreamLakeIngest {

    private StreamLakeIngest() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);
        String serviceUrl = a.getOrDefault("service-url", "pulsar://localhost:6650");
        String adminUrl = a.getOrDefault("admin-url", "http://localhost:8080");
        String tenant = a.getOrDefault("tenant", "public");
        String namespace = a.getOrDefault("namespace", "default");
        String table = a.getOrDefault("table", "Person");
        final int rowsPerPage = Integer.parseInt(a.getOrDefault("rows-per-page", "1000"));
        final long startId = Long.parseLong(a.getOrDefault("start-id", "0"));
        long maxRows = a.containsKey("rows") ? Long.parseLong(a.get("rows")) : Long.MAX_VALUE;
        final long targetBytes = a.containsKey("target-gb")
                ? (long) (Double.parseDouble(a.get("target-gb")) * (1L << 30)) : -1;
        final int threads = Math.max(1, Integer.parseInt(a.getOrDefault("threads", "8")));
        long clientMemMb = Long.parseLong(a.getOrDefault("client-mem-mb", "512"));
        // Bounded in-flight queue per producer = real backpressure (see the producer builder below).
        final int maxPending = Math.max(1, Integer.parseInt(a.getOrDefault("max-pending", "1000")));

        final boolean isPerson = table.equalsIgnoreCase("Person");
        if (!isPerson && !table.equalsIgnoreCase("Employee")) {
            throw new IllegalArgumentException("--table must be Person or Employee, was " + table);
        }
        final String topic = "persistent://" + tenant + "/" + namespace + "/" + table;
        final StreamLakeTopicSchema ts = new StreamLakeTopicSchema(
                isPerson ? personSchema() : employeeSchema(), Arrays.asList(0, 1, 2), 64, 0.01);

        System.out.printf("StreamLake ingest -> %s  (target=%s, threads=%d, rowsPerPage=%d, startId=%d)%n",
                topic, targetBytes > 0 ? gb(targetBytes) : (maxRows + " rows"), threads, rowsPerPage, startId);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl)
                        .ioThreads(threads).memoryLimit(clientMemMb, SizeUnit.MEGA_BYTES).build();
                PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) {

            // Idempotent re-runs: if already at/over the size target, do nothing (no duplicate ids).
            if (targetBytes > 0 && storageSize(admin, topic) >= targetBytes) {
                System.out.printf("%s already at %s (>= target %s); nothing to do.%n",
                        table, gb(storageSize(admin, topic)), gb(targetBytes));
                return;
            }

            final AtomicBoolean stop = new AtomicBoolean(false);
            final AtomicLong written = new AtomicLong(0);
            final long t0 = System.nanoTime();
            final long perThreadMax = maxRows == Long.MAX_VALUE ? Long.MAX_VALUE : maxRows / threads;
            final long finalMaxRows = maxRows;

            // Monitor: poll storage/rows, print throughput, and set the stop flag at the target.
            Thread monitor = new Thread(() -> {
                while (!stop.get()) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        return;
                    }
                    long w = written.get();
                    long storage = targetBytes > 0 ? storageSize(admin, topic) : -1;
                    double secs = (System.nanoTime() - t0) / 1e9;
                    System.out.printf("  %,d rows  %.0f rows/s  %s%n", w, w / Math.max(1e-9, secs),
                            storage >= 0 ? "storage=" + gb(storage) : "");
                    if (targetBytes > 0 && storage >= targetBytes) {
                        stop.set(true);
                    }
                    if (finalMaxRows != Long.MAX_VALUE && w >= finalMaxRows) {
                        stop.set(true);
                    }
                }
            }, "sl-ingest-monitor");
            monitor.setDaemon(true);
            monitor.start();

            // Workers: each writes a strided slice of the id space (unique ids, uniform distribution),
            // each with its own producer so the Arrow encoding + compression run in parallel.
            Thread[] workers = new Thread[threads];
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                workers[t] = new Thread(() -> {
                    try (Producer<byte[]> raw = client.newProducer().topic(topic)
                                    .enableBatching(false)
                                    // Real backpressure: bound the in-flight queue and block the
                                    // ingest thread when it is full, instead of piling millions of
                                    // sends into an unbounded queue. sendTimeout(0) disables the 30s
                                    // send timeout so a broker that is slower than the producers (it now
                                    // does real page-index footer writes on the ack path) throttles us
                                    // rather than failing messages ("Message send timed out").
                                    .maxPendingMessages(maxPending).blockIfQueueFull(true)
                                    .sendTimeout(0, TimeUnit.SECONDS)
                                    // NONE on purpose: StreamLakeProducer self-compresses the Arrow
                                    // region and keeps the stats footer uncompressed at the tail so the
                                    // broker can index the page. Pulsar compression here would bury the
                                    // footer and make the data unqueryable.
                                    .compressionType(CompressionType.NONE).create();
                            StreamLakeProducer p =
                                    new StreamLakeProducer(raw, ts, rowsPerPage, 1 << 30, 0)) {
                        long local = 0;
                        for (long k = startId + tid; !stop.get() && local < perThreadMax; k += threads) {
                            p.addRow(isPerson
                                    ? new Object[]{k, "person-" + k, 20 + (int) (k % 50)}
                                    : new Object[]{9_000_000_000L + k, k, 30_000L + (k % 50) * 1000L});
                            local++;
                            written.incrementAndGet();
                        }
                        p.flush();
                    } catch (Exception e) {
                        System.err.println("ingest worker " + tid + " failed: " + e);
                        stop.set(true);
                    }
                }, "sl-ingest-" + t);
                workers[t].start();
            }
            for (Thread w : workers) {
                w.join();
            }
            stop.set(true);
            monitor.interrupt();

            long total = written.get();
            double secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("DONE %s: wrote %,d rows in %.0fs (%.0f rows/s); storage=%s; nextStartId=%d%n",
                    table, total, secs, total / Math.max(1e-9, secs), gb(storageSize(admin, topic)),
                    startId + total);
        }
    }

    private static long storageSize(PulsarAdmin admin, String topic) {
        try {
            return admin.topics().getStats(topic).getStorageSize();
        } catch (Exception e) {
            return -1;
        }
    }

    private static StreamLakeSchema personSchema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("personId", StreamLakeType.INT64),
                new StreamLakeSchema.Column("name", StreamLakeType.STRING),
                new StreamLakeSchema.Column("age", StreamLakeType.INT32)));
    }

    private static StreamLakeSchema employeeSchema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("empId", StreamLakeType.INT64),
                new StreamLakeSchema.Column("personId", StreamLakeType.INT64),
                new StreamLakeSchema.Column("salary", StreamLakeType.INT64)));
    }

    private static String gb(long bytes) {
        return String.format("%.2f GB", bytes / (double) (1L << 30));
    }

    // Parse "--key value" / "--flag" pairs into a map.
    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    m.put(key, args[++i]);
                } else {
                    m.put(key, "true");
                }
            }
        }
        return m;
    }
}
