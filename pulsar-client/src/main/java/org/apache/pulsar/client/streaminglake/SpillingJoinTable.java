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

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Off-heap build table for hash-join build sides that exceed RAM: only a small key index lives on the
 * JVM heap ({@code key -> list of (offset,length)}), while the row bytes are appended to a spill file
 * (encoded by {@link StreamLakeRowCodec}) and read back on probe. Enabled via
 * {@code joinOffHeapEnabled}; the file is created under {@code joinSpillDir} (a broker-local
 * directory, defaulting to the JVM temp dir) and deleted on {@link #close()}.
 *
 * <p>Not thread-safe (a join owns one table). This is a dependency-free spill; a memory-mapped or
 * Chronicle-Map backend could replace it behind the same {@link StreamLakeJoinTable} interface.
 */
public final class SpillingJoinTable implements StreamLakeJoinTable {

    private final Map<Object, List<long[]>> index = new HashMap<>(); // key -> [{offset, length}]
    private final long maxRows;
    private final Path spillFile;
    private final FileChannel channel;
    private long rows;
    private long writeOffset;

    public SpillingJoinTable(long maxRows, String spillDir) {
        this.maxRows = maxRows;
        try {
            Path dir = (spillDir == null || spillDir.isEmpty())
                    ? Files.createTempDirectory("sl-join")
                    : Paths.get(spillDir);
            Files.createDirectories(dir);
            this.spillFile = Files.createTempFile(dir, "sl-join-", ".spill");
            this.channel = FileChannel.open(spillFile,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake join spill file open failed", e);
        }
    }

    @Override
    public void add(Object key, Object[] row) {
        if (rows >= maxRows) {
            throw new IllegalStateException("StreamLake hash-join build side exceeded " + maxRows
                    + " rows; raise joinMaxBuildRows");
        }
        byte[] bytes = StreamLakeRowCodec.encode(row);
        long offset = writeOffset;
        try {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) {
                writeOffset += channel.write(buf, writeOffset);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake join spill write failed", e);
        }
        index.computeIfAbsent(key, k -> new ArrayList<>()).add(new long[]{offset, bytes.length});
        rows++;
    }

    @Override
    public List<Object[]> get(Object key) {
        List<long[]> locs = index.get(key);
        if (locs == null) {
            return Collections.emptyList();
        }
        List<Object[]> out = new ArrayList<>(locs.size());
        for (long[] loc : locs) {
            out.add(StreamLakeRowCodec.decode(readAt(loc[0], (int) loc[1])));
        }
        return out;
    }

    private byte[] readAt(long offset, int length) {
        ByteBuffer buf = ByteBuffer.allocate(length);
        long pos = offset;
        try {
            while (buf.hasRemaining()) {
                int n = channel.read(buf, pos);
                if (n < 0) {
                    throw new EOFException("StreamLake join spill truncated at " + pos);
                }
                pos += n;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake join spill read failed", e);
        }
        return buf.array();
    }

    @Override
    public long size() {
        return rows;
    }

    /** Total bytes written to the spill file so far (build-side payload spilled off-heap). */
    public long spilledBytes() {
        return writeOffset;
    }

    @Override
    public void close() {
        index.clear();
        try {
            channel.close();
        } catch (IOException ignore) {
            // best-effort
        }
        try {
            Files.deleteIfExists(spillFile);
        } catch (IOException ignore) {
            // best-effort
        }
    }
}
