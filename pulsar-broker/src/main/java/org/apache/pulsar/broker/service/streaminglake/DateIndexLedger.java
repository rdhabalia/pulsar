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

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Per-topic date partition index (stage-1 pruning): maps each day to the ledgers that
 * contain records for that day, so a scan over a date range touches only the relevant
 * ledgers. Updated by the broker when a ledger is closed (via the managed-ledger
 * rollover hook).
 *
 * <p>This holds the in-memory index and append/query semantics. The durable backing is a
 * dedicated append-only BookKeeper ledger per the design; persisting/replaying the
 * index to that ledger is the integration point exercised under a running cluster.
 */
public final class DateIndexLedger {

    /** Metadata recorded for a closed ledger. */
    public static final class LedgerInfo {
        public final long ledgerId;
        public final long startTimestamp;
        public final long endTimestamp;

        public LedgerInfo(long ledgerId, long startTimestamp, long endTimestamp) {
            this.ledgerId = ledgerId;
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
        }
    }

    private final TreeMap<LocalDate, List<LedgerInfo>> partitions = new TreeMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public static LocalDate dateOf(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atZone(ZoneOffset.UTC).toLocalDate();
    }

    /** Record a closed ledger, indexed by the date(s) it spans. Append-only. */
    public void append(LedgerInfo info) {
        LocalDate from = dateOf(info.startTimestamp);
        LocalDate to = dateOf(info.endTimestamp);
        lock.writeLock().lock();
        try {
            for (LocalDate d = from; !d.isAfter(to); d = d.plusDays(1)) {
                partitions.computeIfAbsent(d, k -> new ArrayList<>()).add(info);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Stage-1 pruning: distinct ledgerIds whose date falls within [fromMillis, toMillis]. */
    public List<Long> candidateLedgers(long fromMillis, long toMillis) {
        LocalDate from = dateOf(fromMillis);
        LocalDate to = dateOf(toMillis);
        List<Long> out = new ArrayList<>();
        lock.readLock().lock();
        try {
            for (Map.Entry<LocalDate, List<LedgerInfo>> e
                    : partitions.subMap(from, true, to, true).entrySet()) {
                for (LedgerInfo li : e.getValue()) {
                    if (!out.contains(li.ledgerId)) {
                        out.add(li.ledgerId);
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return out;
    }

    public int partitionCount() {
        lock.readLock().lock();
        try {
            return partitions.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
