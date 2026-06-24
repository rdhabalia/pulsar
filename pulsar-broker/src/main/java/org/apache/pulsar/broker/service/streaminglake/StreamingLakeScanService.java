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

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Server-side Streaming Lake scan over a topic's managed ledger. Reads the topic's
 * stored messages, extracts the indexed columns (name, departmentId, salary) from each
 * message's metadata and its eventTime (the date partition), and returns the rows
 * matching the date range + column predicate. This is the broker-side query execution
 * running against real BookKeeper storage.
 *
 * <p>The unbounded sentinels ({@code Integer.MIN_VALUE}/{@code MAX_VALUE},
 * {@code Long.MIN_VALUE}/{@code MAX_VALUE}) express "no bound", so a full
 * {@code SELECT *} is just a scan with every bound unbounded.
 */
public final class StreamingLakeScanService {

    private StreamingLakeScanService() {
    }

    /** A scanned row: the indexed columns from a message's metadata. */
    public static final class Row {
        public final String name;
        public final int departmentId;
        public final int salary;
        public final long eventTime;

        Row(String name, int departmentId, int salary, long eventTime) {
            this.name = name;
            this.departmentId = departmentId;
            this.salary = salary;
            this.eventTime = eventTime;
        }

        @Override
        public String toString() {
            return name + "," + departmentId + "," + salary + "," + eventTime;
        }
    }

    /**
     * SELECT * FROM topic
     * WHERE eventTime BETWEEN fromTime AND toTime
     *   AND departmentId > deptMin AND departmentId < deptMax AND salary > salaryMin
     */
    public static List<Row> scan(ManagedLedger ledger, long fromTime, long toTime,
                                 int deptMin, int deptMax, int salaryMin) throws Exception {
        final List<Row> out = new ArrayList<>();
        final ManagedCursor cursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST);
        try {
            while (cursor.hasMoreEntries()) {
                List<Entry> entries = cursor.readEntries(100);
                for (Entry entry : entries) {
                    try {
                        MessageMetadata md = entry.getMessageMetadata();
                        if (md == null) {
                            ByteBuf buf = entry.getDataBuffer();
                            md = Commands.peekMessageMetadata(buf, "streaming-lake-scan", -1);
                        }
                        if (md == null || md.hasMarkerType()) {
                            continue; // skip server-only marker entries
                        }

                        String name = null;
                        int dept = Integer.MIN_VALUE;
                        int salary = Integer.MIN_VALUE;
                        for (KeyValue kv : md.getPropertiesList()) {
                            String k = kv.getKey();
                            if ("name".equals(k)) {
                                name = kv.getValue();
                            } else if ("departmentId".equals(k)) {
                                dept = Integer.parseInt(kv.getValue());
                            } else if ("salary".equals(k)) {
                                salary = Integer.parseInt(kv.getValue());
                            }
                        }
                        long eventTime = md.getEventTime();

                        if (eventTime >= fromTime && eventTime <= toTime
                                && dept > deptMin && dept < deptMax && salary > salaryMin) {
                            out.add(new Row(name, dept, salary, eventTime));
                        }
                    } finally {
                        entry.release();
                    }
                }
            }
        } finally {
            cursor.close();
        }
        return out;
    }
}
