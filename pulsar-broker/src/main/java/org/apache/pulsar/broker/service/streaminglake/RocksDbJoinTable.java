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

import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.client.streaminglake.StreamLakeJoinTable;
import org.apache.pulsar.client.streaminglake.StreamLakeRowCodec;
import org.rocksdb.RocksIterator;

/**
 * A hash-join build table backed by {@link RocksDbStore}: unlike {@link
 * org.apache.pulsar.client.streaminglake.SpillingJoinTable} (whose key index stays on the JVM heap),
 * RocksDB keeps <b>both keys and values on disk</b> (only the block cache + memtables are off-heap and
 * bounded), so the build side can scale to billions of rows on a small-heap query broker.
 *
 * <p>Each build row is stored under a composite key {@code keyBytes ‖ seq}: {@code keyBytes} is the
 * deterministic, self-delimited encoding of the (non-null) join-key value (via {@link
 * StreamLakeRowCodec}), and {@code seq} is an 8-byte increasing suffix so one key can hold many rows
 * (e.g. a hot key) without a merge operator. {@link #get(Object)} prefix-seeks {@code keyBytes} and
 * iterates the contiguous run of that key's rows (sorted SSTs + prefix bloom make this fast). This is
 * the {@code ROCKSDB} join strategy: build the smaller side into RocksDB, stream the probe.
 */
public final class RocksDbJoinTable implements StreamLakeJoinTable {

    private final RocksDbStore store;
    private long seq;
    private long rows;

    public RocksDbJoinTable(String spillDir, long blockCacheBytes, long writeBufferBytes) {
        this.store = RocksDbStore.open(spillDir, blockCacheBytes, writeBufferBytes);
    }

    @Override
    public void add(Object key, Object[] row) {
        byte[] prefix = keyBytes(key);
        byte[] composite = new byte[prefix.length + 8];
        System.arraycopy(prefix, 0, composite, 0, prefix.length);
        long s = seq++;
        for (int i = 0; i < 8; i++) {
            composite[prefix.length + i] = (byte) (s >>> (56 - 8 * i));
        }
        store.put(composite, StreamLakeRowCodec.encode(row));
        rows++;
    }

    @Override
    public List<Object[]> get(Object key) {
        byte[] prefix = keyBytes(key);
        List<Object[]> out = new ArrayList<>();
        try (RocksIterator it = store.newIterator()) {
            it.seek(prefix);
            while (it.isValid()) {
                byte[] k = it.key();
                if (!startsWith(k, prefix)) {
                    break;
                }
                out.add(StreamLakeRowCodec.decode(it.value()));
                it.next();
            }
        }
        return out;
    }

    @Override
    public long size() {
        return rows;
    }

    @Override
    public void close() {
        store.close();
    }

    // Deterministic, self-delimited key encoding: equal values -> equal bytes; no value's encoding is a
    // prefix of another's (the row codec length-prefixes each cell), so prefix scans never cross keys.
    private static byte[] keyBytes(Object key) {
        return StreamLakeRowCodec.encode(new Object[]{key});
    }

    private static boolean startsWith(byte[] a, byte[] prefix) {
        if (a.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (a[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
