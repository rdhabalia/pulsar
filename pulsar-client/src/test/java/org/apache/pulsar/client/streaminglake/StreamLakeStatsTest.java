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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

/** Tests for the per-batch stats builder, footer codec, membership/range pruning, and payload framing. */
public class StreamLakeStatsTest {

    private static StreamLakeSchema schema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("id", StreamLakeType.INT32),
                new StreamLakeSchema.Column("deptId", StreamLakeType.INT32),    // low-cardinality
                new StreamLakeSchema.Column("email", StreamLakeType.STRING)));   // high-cardinality
    }

    private static List<Object[]> rows(int n) {
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            rows.add(new Object[]{i, i % 5, "user-" + i + "@x.com"});
        }
        return rows;
    }

    private static byte[] enc(StreamLakeType type, Object v) {
        return StreamLakeOrderPreserving.encode(type, v);
    }

    @Test
    public void lowCardGetsExactSetHighCardGetsBloom() {
        StreamLakeBatchStats stats = StreamLakeStatsBuilder.build(
                schema(), rows(200), Arrays.asList(1, 2), 64, 0.01);

        StreamLakeBatchStats.ColumnStats dept = stats.column(1);
        assertNotNull(dept.min);
        assertNotNull(dept.exactSet, "deptId (5 distinct) -> exact set");
        assertNull(dept.bloom);
        assertEquals(dept.distinctCount, 5);

        StreamLakeBatchStats.ColumnStats email = stats.column(2);
        assertNull(email.exactSet, "email (200 distinct) -> bloom");
        assertNotNull(email.bloom);
        assertEquals(email.distinctCount, 200);
    }

    @Test
    public void membershipAndRangePruning() {
        StreamLakeBatchStats stats = StreamLakeStatsBuilder.build(
                schema(), rows(200), Arrays.asList(1, 2), 64, 0.01);
        StreamLakeBatchStats.ColumnStats dept = stats.column(1); // deptId values 0..4
        assertTrue(dept.mightContain(enc(StreamLakeType.INT32, 3)));
        assertFalse(dept.mightContain(enc(StreamLakeType.INT32, 99)));
        assertTrue(dept.overlaps(enc(StreamLakeType.INT32, 2), enc(StreamLakeType.INT32, 10)));
        assertFalse(dept.overlaps(enc(StreamLakeType.INT32, 100), enc(StreamLakeType.INT32, 200)));

        StreamLakeBatchStats.ColumnStats email = stats.column(2);
        assertTrue(email.mightContain(enc(StreamLakeType.STRING, "user-42@x.com")));
    }

    @Test
    public void footerCodecRoundTrips() {
        StreamLakeBatchStats stats = StreamLakeStatsBuilder.build(
                schema(), rows(200), Arrays.asList(0, 1, 2), 64, 0.01);
        StreamLakeBatchStats back = StreamLakeBatchStats.decode(stats.encode());
        assertEquals(back.columns().size(), 3);

        StreamLakeBatchStats.ColumnStats dept = back.column(1);
        assertEquals(dept.distinctCount, 5);
        assertNotNull(dept.exactSet);
        assertTrue(dept.mightContain(enc(StreamLakeType.INT32, 3)));

        StreamLakeBatchStats.ColumnStats email = back.column(2);
        assertNotNull(email.bloom);
        assertTrue(email.mightContain(enc(StreamLakeType.STRING, "user-1@x.com")));
    }

    @Test
    public void payloadFramingSplitsBack() {
        byte[] arrow = "ARROW-IPC-BYTES".getBytes();
        StreamLakeBatchStats stats = StreamLakeStatsBuilder.build(
                schema(), rows(10), Arrays.asList(1), 64, 0.01);
        byte[] footer = stats.encode();
        byte[] payload = StreamLakeBatchPayload.combine(arrow, footer);

        assertTrue(StreamLakeBatchPayload.hasFooter(payload));
        assertEquals(StreamLakeBatchPayload.arrowBatch(payload), arrow);
        assertEquals(StreamLakeBatchPayload.statsFooter(payload), footer);
    }
}
