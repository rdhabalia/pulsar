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
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

/** Per-column, per-page segment: exact-page pruning by range/bloom, collapse over cap, round-trip. */
public class StreamLakeColumnSegmentTest {

    private static final StreamLakeSchema SCHEMA = new StreamLakeSchema(Arrays.asList(
            new StreamLakeSchema.Column("id", StreamLakeType.INT32),
            new StreamLakeSchema.Column("name", StreamLakeType.STRING)));

    // A page's ColumnStats for one column, over ids [from, to] with names "n<i>".
    private static StreamLakeBatchStats.ColumnStats page(int column, int from, int to) {
        List<Object[]> rows = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            rows.add(new Object[]{i, "n" + i});
        }
        return StreamLakeStatsBuilder.build(SCHEMA, rows, Arrays.asList(0, 1), 64, 0.01).column(column);
    }

    private static StreamLakeScanPredicate.ColumnPredicate eq(int col, StreamLakeType type, Object v) {
        return StreamLakeScanPredicate.builder().eq(col, type, v).build().columns().get(0);
    }

    @Test
    public void numericPerPageRangePrunesToExactPage() {
        List<StreamLakeBatchStats.ColumnStats> perPage = Arrays.asList(
                page(0, 0, 9), page(0, 10, 19), page(0, 20, 29));
        StreamLakeColumnSegment seg = StreamLakeColumnSegment.build(0, StreamLakeType.INT32, perPage,
                2L * 1024 * 1024, 0.01);
        assertFalse(seg.collapsed());
        assertEquals(seg.numPages(), 3);

        boolean[] pos = seg.candidatePositions(eq(0, StreamLakeType.INT32, 15));
        assertFalse(pos[0]);
        assertTrue(pos[1], "15 is only in page [10,19]");
        assertFalse(pos[2]);

        boolean[] wide = seg.candidatePositions(StreamLakeScanPredicate.builder()
                .range(0, StreamLakeType.INT32, 5, 25).build().columns().get(0));
        assertTrue(wide[0] && wide[1] && wide[2], "range [5,25] overlaps every page");
    }

    @Test
    public void textKeepsPerPageBloomForMembership() {
        List<StreamLakeBatchStats.ColumnStats> perPage = Arrays.asList(
                page(1, 0, 1), page(1, 100, 101));
        StreamLakeColumnSegment seg = StreamLakeColumnSegment.build(1, StreamLakeType.STRING, perPage,
                2L * 1024 * 1024, 0.01);
        assertFalse(seg.collapsed());

        boolean[] pos = seg.candidatePositions(eq(1, StreamLakeType.STRING, "n0"));
        assertTrue(pos[0], "page 0 holds n0");
        assertFalse(pos[1], "page 1 (n100,n101) excluded");
    }

    @Test
    public void collapsesPastTheByteCap() {
        List<StreamLakeBatchStats.ColumnStats> perPage = Arrays.asList(
                page(0, 0, 9), page(0, 10, 19));
        // A 1-byte cap forces the column to collapse to one whole-segment stat.
        StreamLakeColumnSegment seg = StreamLakeColumnSegment.build(0, StreamLakeType.INT32, perPage,
                1, 0.01);
        assertTrue(seg.collapsed());

        // Union min/max is [0,19]: a value inside keeps all positions; outside drops them all.
        boolean[] inside = seg.candidatePositions(eq(0, StreamLakeType.INT32, 5));
        assertTrue(inside[0] && inside[1]);
        boolean[] outside = seg.candidatePositions(eq(0, StreamLakeType.INT32, 100));
        assertFalse(outside[0] || outside[1]);
    }

    @Test
    public void encodeDecodeRoundTrip() {
        List<StreamLakeBatchStats.ColumnStats> perPage = Arrays.asList(
                page(1, 0, 1), page(1, 100, 101));
        StreamLakeColumnSegment seg = StreamLakeColumnSegment.build(1, StreamLakeType.STRING, perPage,
                2L * 1024 * 1024, 0.01);
        StreamLakeColumnSegment decoded = StreamLakeColumnSegment.decode(seg.encode());
        assertEquals(decoded.columnIndex(), 1);
        assertEquals(decoded.numPages(), 2);

        boolean[] pos = decoded.candidatePositions(eq(1, StreamLakeType.STRING, "n0"));
        assertTrue(pos[0]);
        assertFalse(pos[1]);
    }
}
