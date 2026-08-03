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
package org.apache.pulsar.broker.service.streaminglake;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.streaminglake.StreamLakeHashJoin;
import org.apache.pulsar.client.streaminglake.StreamLakeRowCodec;
import org.apache.pulsar.client.streaminglake.StreamLakeScanPredicate;

/**
 * Out-of-core inner join for build sides larger than memory (the "#4" Grace / partitioned hash join).
 * Instead of holding one whole side in memory (the broadcast join), it does two passes:
 *
 * <ol>
 *   <li><b>Partition pass</b> — scan BOTH sides once and write each row to one of {@code N} on-disk
 *       partition files chosen by {@code hash(joinKey) % N}. Equal keys hash to the same partition on
 *       both sides, so a key's matches are always co-located in the same partition index.</li>
 *   <li><b>Join pass</b> — for each partition {@code p}, load only {@code build_p} into an in-memory
 *       hash table, stream {@code probe_p}, and emit {@code concat(probeRow, buildRow)} per match. Only
 *       one partition's build side is resident at a time, so peak memory is {@code O(total / N)}.</li>
 * </ol>
 *
 * <p>Choose {@code N} (from the planner's size estimate) so a single partition's build side fits the
 * build-memory budget. Rows are (de)serialized with {@link StreamLakeRowCodec}; all spill files live
 * under a temp directory that is deleted when the join finishes. Emits the same
 * {@code concat(probeRow, buildRow)} shape as the broadcast join, so the coordinator's row combiner is
 * unchanged.
 *
 * <p><b>Skew</b> is not yet handled here: a single hot key that overflows its partition throws (raise
 * the partition count) — heavy-hitter-driven broadcast/salting is the planned follow-on.
 */
public final class StreamLakeGraceJoin {

    private StreamLakeGraceJoin() {
    }

    /**
     * Run the partitioned join, pushing each {@code concat(probeRow, buildRow)} to {@code out}.
     *
     * @param build         executor for the (smaller, but still &gt; memory) build side
     * @param probe         executor for the probe side
     * @param partitions    number of on-disk partitions {@code N} (&ge; 1)
     * @param spillDir      directory for partition files (empty = JVM temp); point at the query NVMe
     * @param maxPartitionBuildRows per-partition build-side admission guard (skew protection)
     */
    public static void join(
            StreamLakeQueryExecutor build, long fromMs, long toMs,
            StreamLakeScanPredicate buildPredicate, int buildKey,
            StreamLakeQueryExecutor probe, StreamLakeScanPredicate probePredicate, int probeKey,
            int partitions, String spillDir, long maxPartitionBuildRows,
            StreamLakeQueryExecutor.RowConsumer out) throws Exception {
        int n = Math.max(1, partitions);
        Path dir = (spillDir == null || spillDir.isEmpty())
                ? Files.createTempDirectory("sl-grace")
                : Files.createTempDirectory(Paths.get(spillDir), "sl-grace");
        Path[] buildPath = new Path[n];
        Path[] probePath = new Path[n];
        DataOutputStream[] buildOut = new DataOutputStream[n];
        DataOutputStream[] probeOut = new DataOutputStream[n];
        try {
            for (int p = 0; p < n; p++) {
                buildPath[p] = dir.resolve("b" + p);
                probePath[p] = dir.resolve("p" + p);
                buildOut[p] = openOut(buildPath[p]);
                probeOut[p] = openOut(probePath[p]);
            }

            // Partition pass: hash each filtered row to its partition file (null keys never match).
            build.scan(fromMs, toMs, buildPredicate, row -> {
                Object key = row[buildKey];
                if (key != null) {
                    writeRow(buildOut[partition(key, n)], row);
                }
            });
            probe.scan(fromMs, toMs, probePredicate, row -> {
                Object key = row[probeKey];
                if (key != null) {
                    writeRow(probeOut[partition(key, n)], row);
                }
            });
            for (int p = 0; p < n; p++) {
                buildOut[p].close();
                buildOut[p] = null;
                probeOut[p].close();
                probeOut[p] = null;
            }

            // Join pass: one partition resident at a time.
            for (int p = 0; p < n; p++) {
                final int part = p;
                Map<Object, List<Object[]>> table = new HashMap<>();
                long[] rows = {0};
                forEachRow(buildPath[p], row -> {
                    if (++rows[0] > maxPartitionBuildRows) {
                        throw new IllegalStateException("StreamLake grace-join partition " + part
                                + " build side exceeded " + maxPartitionBuildRows
                                + " rows; raise the partition count or enable skew handling");
                    }
                    table.computeIfAbsent(row[buildKey], k -> new ArrayList<>()).add(row);
                });
                forEachRow(probePath[p], probeRow -> {
                    List<Object[]> matches = table.get(probeRow[probeKey]);
                    if (matches != null) {
                        for (Object[] buildRow : matches) {
                            out.accept(StreamLakeHashJoin.concat(probeRow, buildRow));
                        }
                    }
                });
            }
        } finally {
            for (int p = 0; p < n; p++) {
                closeQuietly(buildOut[p]);
                closeQuietly(probeOut[p]);
            }
            deleteQuietly(buildPath);
            deleteQuietly(probePath);
            try {
                Files.deleteIfExists(dir);
            } catch (IOException ignore) {
                // best-effort
            }
        }
    }

    private static int partition(Object key, int n) {
        return (key.hashCode() & 0x7fffffff) % n;
    }

    private static DataOutputStream openOut(Path path) throws IOException {
        return new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
    }

    private static void writeRow(DataOutputStream out, Object[] row) throws IOException {
        byte[] blob = StreamLakeRowCodec.encode(row);
        out.writeInt(blob.length);
        out.write(blob);
    }

    private static void forEachRow(Path path, StreamLakeQueryExecutor.RowConsumer consumer)
            throws Exception {
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(path)))) {
            while (true) {
                int len;
                try {
                    len = in.readInt();
                } catch (EOFException eof) {
                    return;
                }
                byte[] blob = new byte[len];
                in.readFully(blob);
                consumer.accept(StreamLakeRowCodec.decode(blob));
            }
        }
    }

    private static void closeQuietly(DataOutputStream out) {
        if (out != null) {
            try {
                out.close();
            } catch (IOException ignore) {
                // best-effort
            }
        }
    }

    private static void deleteQuietly(Path[] paths) {
        for (Path path : paths) {
            if (path != null) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ignore) {
                    // best-effort
                }
            }
        }
    }
}
