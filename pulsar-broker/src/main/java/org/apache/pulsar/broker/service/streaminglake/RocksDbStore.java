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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

/**
 * A short-lived, <b>ephemeral</b> RocksDB store used as the on-NVMe spill engine for out-of-core query
 * operators (hash-join build table, external sort, group-by aggregation). It is deliberately tuned for a
 * throwaway workload:
 *
 * <ul>
 *   <li><b>WAL disabled</b> — the data is scratch for a single query; durability is not needed.</li>
 *   <li><b>bounded off-heap memory</b> — an explicit LRU block cache and write-buffer size, because
 *       RocksDB's cache + memtables live in native memory (outside {@code -Xmx}); this keeps process RSS
 *       predictable on the query broker.</li>
 *   <li><b>default bytewise comparator</b> — callers make their keys order-preserving (see
 *       {@link StreamLakeOrderKeyCodec}) rather than pay for a JNI custom comparator.</li>
 * </ul>
 *
 * <p>The database lives in a fresh temp directory under the configured spill dir and is <b>deleted on
 * {@link #close()}</b>. Not thread-safe (one operator owns one store).
 */
public final class RocksDbStore implements AutoCloseable {

    private final RocksDB db;
    private final Path dir;
    private final Options options;
    private final WriteOptions writeOptions;
    private final Cache blockCache;

    private RocksDbStore(RocksDB db, Path dir, Options options, WriteOptions writeOptions, Cache blockCache) {
        this.db = db;
        this.dir = dir;
        this.options = options;
        this.writeOptions = writeOptions;
        this.blockCache = blockCache;
    }

    /**
     * Open a fresh store under {@code baseDir} (JVM temp if empty), with a bounded off-heap block cache
     * and write buffer.
     */
    public static RocksDbStore open(String baseDir, long blockCacheBytes, long writeBufferBytes) {
        RocksDB.loadLibrary();
        Path dir;
        try {
            Path base = (baseDir == null || baseDir.isEmpty()) ? null : Paths.get(baseDir);
            dir = base == null ? Files.createTempDirectory("sl-rocks")
                    : Files.createTempDirectory(Files.createDirectories(base), "sl-rocks");
        } catch (IOException e) {
            throw new UncheckedIOException("StreamLake RocksDB temp dir create failed", e);
        }
        Cache cache = new LRUCache(Math.max(8L << 20, blockCacheBytes));
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setBlockCache(cache)
                .setCacheIndexAndFilterBlocks(true);
        Options options = new Options()
                .setCreateIfMissing(true)
                .setTableFormatConfig(tableConfig)
                .setWriteBufferSize(Math.max(8L << 20, writeBufferBytes))
                .setMaxWriteBufferNumber(2)
                .setCompressionType(CompressionType.LZ4_COMPRESSION);
        WriteOptions writeOptions = new WriteOptions().setDisableWAL(true);
        try {
            RocksDB db = RocksDB.open(options, dir.toString());
            return new RocksDbStore(db, dir, options, writeOptions, cache);
        } catch (RocksDBException e) {
            options.close();
            writeOptions.close();
            cache.close();
            deleteDir(dir);
            throw new IllegalStateException("StreamLake RocksDB open failed: " + e.getMessage(), e);
        }
    }

    public void put(byte[] key, byte[] value) {
        try {
            db.put(writeOptions, key, value);
        } catch (RocksDBException e) {
            throw new IllegalStateException("StreamLake RocksDB put failed: " + e.getMessage(), e);
        }
    }

    public byte[] get(byte[] key) {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new IllegalStateException("StreamLake RocksDB get failed: " + e.getMessage(), e);
        }
    }

    /** A fresh iterator over the whole keyspace (bytewise key order). Caller must close it. */
    public RocksIterator newIterator() {
        return db.newIterator();
    }

    @Override
    public void close() {
        try {
            db.close();
        } finally {
            writeOptions.close();
            options.close();
            blockCache.close();
            deleteDir(dir);
        }
    }

    private static void deleteDir(Path dir) {
        if (dir == null) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignore) {
                    // best-effort
                }
            });
        } catch (IOException ignore) {
            // best-effort
        }
    }
}
