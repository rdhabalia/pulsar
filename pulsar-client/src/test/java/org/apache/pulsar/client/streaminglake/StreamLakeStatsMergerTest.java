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
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

/** Merges per-batch stats into a segment summary: union sets for low-card, min/max only for high-card. */
public class StreamLakeStatsMergerTest {

    private static StreamLakeSchema schema() {
        return new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("deptId", StreamLakeType.INT32),   // low-card
                new StreamLakeSchema.Column("email", StreamLakeType.STRING)));  // high-card
    }

    private static byte[] enc(StreamLakeType t, Object v) {
        return StreamLakeOrderPreserving.encode(t, v);
    }

    private static StreamLakeBatchStats statsFor(int fromDept, int toDept, int emailBase, int emailCount) {
        List<Object[]> rows = new ArrayList<>();
        for (int d = fromDept; d <= toDept; d++) {
            rows.add(new Object[]{d, "e" + (emailBase++) + "@x.com"});
        }
        for (int i = 1; i < emailCount; i++) {
            rows.add(new Object[]{fromDept, "e" + (emailBase++) + "@x.com"});
        }
        return StreamLakeStatsBuilder.build(schema(), rows, Arrays.asList(0, 1), 64, 0.01);
    }

    @Test
    public void unionsLowCardSetsAndKeepsHighCardRangeOnly() {
        // three batches with deptId ranges 0-2, 3-5, 6-8 (9 distinct total, under cap) and >cap emails
        // per batch (so each batch stores an email bloom, not a set -> the merge can't rebuild a set).
        List<StreamLakeBatchStats> parts = Arrays.asList(
                statsFor(0, 2, 0, 70), statsFor(3, 5, 100, 70), statsFor(6, 8, 200, 70));
        StreamLakeBatchStats merged = StreamLakeStatsMerger.merge(parts, 64, 0.01);

        StreamLakeBatchStats.ColumnStats dept = merged.column(0);
        assertNotNull(dept.min);
        assertEquals(dept.distinctCount(), 9, "deptId union across batches = 9 distinct");
        // exact-set membership preserved across the merge (no false negatives)
        assertTrue(dept.mightContain(enc(StreamLakeType.INT32, 0)));
        assertTrue(dept.mightContain(enc(StreamLakeType.INT32, 8)));
        assertFalse(dept.mightContain(enc(StreamLakeType.INT32, 42)));
        // range spans all batches
        assertTrue(dept.overlaps(enc(StreamLakeType.INT32, 5), enc(StreamLakeType.INT32, 5)));

        StreamLakeBatchStats.ColumnStats email = merged.column(1);
        assertNotNull(email.min, "high-card keeps min/max for range pruning");
        assertEquals(email.distinctCount(), -1, "high-card email has no merged set");
    }

    @Test
    public void promotesLargeUnionToASegmentBloom() {
        // deptId distinct across batches exceeds the small cap -> merged column becomes a bloom.
        List<StreamLakeBatchStats> parts = Arrays.asList(
                statsFor(0, 9, 0, 1), statsFor(10, 19, 100, 1), statsFor(20, 29, 200, 1));
        StreamLakeBatchStats merged = StreamLakeStatsMerger.merge(parts, 8, 0.01);

        StreamLakeBatchStats.ColumnStats dept = merged.column(0);
        assertEquals(dept.distinctCount(), 30, "30 distinct deptIds across batches");
        // above the cap of 8 -> stored as a bloom, still no false negatives
        assertTrue(dept.mightContain(enc(StreamLakeType.INT32, 15)));
        assertTrue(dept.mightContain(enc(StreamLakeType.INT32, 29)));
    }

    @Test
    public void mergeRoundTripsThroughTheFooterCodec() {
        List<StreamLakeBatchStats> parts = Arrays.asList(statsFor(0, 3, 0, 20), statsFor(4, 7, 50, 20));
        StreamLakeBatchStats merged = StreamLakeStatsMerger.merge(parts, 64, 0.01);
        StreamLakeBatchStats back = StreamLakeBatchStats.decode(merged.encode());
        assertEquals(back.column(0).distinctCount(), 8);
        assertTrue(back.column(0).mightContain(enc(StreamLakeType.INT32, 7)));
        assertNotNull(back.column(1).min, "email retains min/max after codec");
    }
}
