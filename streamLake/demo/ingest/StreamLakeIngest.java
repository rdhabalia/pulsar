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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
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
        int rowsPerPage = Integer.parseInt(a.getOrDefault("rows-per-page", "1000"));
        long startId = Long.parseLong(a.getOrDefault("start-id", "0"));
        long maxRows = a.containsKey("rows") ? Long.parseLong(a.get("rows")) : Long.MAX_VALUE;
        long targetBytes = a.containsKey("target-gb")
                ? (long) (Double.parseDouble(a.get("target-gb")) * (1L << 30)) : -1;
        long reportEvery = Long.parseLong(a.getOrDefault("report-every", "1000000"));

        boolean isPerson = table.equalsIgnoreCase("Person");
        if (!isPerson && !table.equalsIgnoreCase("Employee")) {
            throw new IllegalArgumentException("--table must be Person or Employee, was " + table);
        }
        String topic = "persistent://" + tenant + "/" + namespace + "/" + table;
        StreamLakeTopicSchema ts = new StreamLakeTopicSchema(
                isPerson ? personSchema() : employeeSchema(), Arrays.asList(0, 1, 2), 64, 0.01);

        System.out.printf("StreamLake ingest -> %s  (target=%s, rowsPerPage=%d, startId=%d)%n",
                topic, targetBytes > 0 ? gb(targetBytes) : (maxRows + " rows"), rowsPerPage, startId);

        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).build();
                PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();
                Producer<byte[]> raw = client.newProducer().topic(topic)
                        .enableBatching(false).blockIfQueueFull(true)
                        .compressionType(CompressionType.ZSTD).create();
                StreamLakeProducer p = new StreamLakeProducer(raw, ts, rowsPerPage, 1 << 30, 0)) {

            long t0 = System.nanoTime();
            long i = startId;
            long written = 0;
            long lastReport = 0;
            while (written < maxRows) {
                p.addRow(isPerson
                        ? new Object[]{i, "person-" + i, 20 + (int) (i % 50)}
                        : new Object[]{9_000_000_000L + i, i, 30_000L + (i % 50) * 1000L});
                i++;
                written++;
                if (written - lastReport >= reportEvery) {
                    lastReport = written;
                    long storage = targetBytes > 0 ? storageSize(admin, topic) : -1;
                    double secs = (System.nanoTime() - t0) / 1e9;
                    System.out.printf("  %,d rows  %.0f rows/s  %s%n", written, written / secs,
                            storage >= 0 ? "storage=" + gb(storage) : "");
                    if (targetBytes > 0 && storage >= targetBytes) {
                        break;
                    }
                }
            }
            p.flush();
            double secs = (System.nanoTime() - t0) / 1e9;
            long finalStorage = storageSize(admin, topic);
            System.out.printf("DONE %s: wrote %,d rows in %.0fs (%.0f rows/s); storage=%s; nextStartId=%d%n",
                    table, written, secs, written / secs, gb(finalStorage), i);
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
