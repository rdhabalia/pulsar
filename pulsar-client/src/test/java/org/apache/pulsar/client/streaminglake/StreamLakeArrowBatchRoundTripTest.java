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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

/** Round-trip: StreamLake rows -> Arrow IPC batch -> rows, across all column types and nulls. */
public class StreamLakeArrowBatchRoundTripTest {

    private static StreamLakeSchema employeeSchema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("id", StreamLakeType.INT32),
                new StreamLakeSchema.Column("name", StreamLakeType.STRING),
                new StreamLakeSchema.Column("salary", StreamLakeType.INT64),
                new StreamLakeSchema.Column("score", StreamLakeType.DOUBLE),
                new StreamLakeSchema.Column("active", StreamLakeType.BOOLEAN),
                new StreamLakeSchema.Column("blob", StreamLakeType.BYTES)));
    }

    @Test
    public void roundTripsAllColumnTypes() {
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1, "John", 100000L, 4.5d, true, new byte[]{1, 2, 3}});
        rows.add(new Object[]{2, "Doe", 200000L, 9.25d, false, new byte[]{}});
        rows.add(new Object[]{3, "Sam", 50000L, -1.0d, true, new byte[]{9}});

        byte[] ipc;
        try (StreamLakeArrowBatchEncoder encoder = new StreamLakeArrowBatchEncoder(employeeSchema())) {
            ipc = encoder.encode(rows);
        }

        List<Object[]> out;
        try (StreamLakeArrowBatchDecoder decoder = new StreamLakeArrowBatchDecoder()) {
            out = decoder.decodeRows(ipc);
        }

        assertEquals(out.size(), 3);
        assertEquals(out.get(0)[0], 1);
        assertEquals(out.get(0)[1], "John");
        assertEquals(out.get(0)[2], 100000L);
        assertEquals(out.get(0)[3], 4.5d);
        assertEquals(out.get(0)[4], true);
        assertEquals((byte[]) out.get(0)[5], new byte[]{1, 2, 3});
        assertEquals(out.get(1)[4], false);
        assertEquals((byte[]) out.get(1)[5], new byte[]{});
        assertEquals(out.get(2)[1], "Sam");
        assertEquals(out.get(2)[3], -1.0d);
    }

    @Test
    public void preservesNulls() {
        StreamLakeSchema schema = new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("id", StreamLakeType.INT32),
                new StreamLakeSchema.Column("name", StreamLakeType.STRING)));
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{10, null});
        rows.add(new Object[]{null, "present"});

        byte[] ipc;
        try (StreamLakeArrowBatchEncoder encoder = new StreamLakeArrowBatchEncoder(schema)) {
            ipc = encoder.encode(rows);
        }
        List<Object[]> out;
        try (StreamLakeArrowBatchDecoder decoder = new StreamLakeArrowBatchDecoder()) {
            out = decoder.decodeRows(ipc);
        }

        assertEquals(out.size(), 2);
        assertEquals(out.get(0)[0], 10);
        assertNull(out.get(0)[1]);
        assertNull(out.get(1)[0]);
        assertEquals(out.get(1)[1], "present");
    }
}
