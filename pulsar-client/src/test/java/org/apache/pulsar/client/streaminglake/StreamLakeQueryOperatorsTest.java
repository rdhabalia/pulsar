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

/** Late-materialization decode, broadcast hash join, and bounded top-K query operators. */
public class StreamLakeQueryOperatorsTest {

    @Test
    public void lateMaterializationDecodesOnlyRequestedColumns() {
        StreamLakeSchema schema = new StreamLakeSchema(Arrays.asList(
                new StreamLakeSchema.Column("id", StreamLakeType.INT32),
                new StreamLakeSchema.Column("name", StreamLakeType.STRING),
                new StreamLakeSchema.Column("salary", StreamLakeType.INT64)));
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{1, "John", 100L});
        rows.add(new Object[]{2, "Doe", 200L});
        byte[] ipc;
        try (StreamLakeArrowBatchEncoder enc = new StreamLakeArrowBatchEncoder(schema)) {
            ipc = enc.encode(rows);
        }
        try (StreamLakeArrowBatchDecoder dec = new StreamLakeArrowBatchDecoder()) {
            List<Object> ids = dec.decodeColumn(ipc, 0);
            assertEquals(ids, Arrays.asList(1, 2));
            List<Object[]> idSalary = dec.decodeColumns(ipc, new int[]{0, 2});
            assertEquals(idSalary.size(), 2);
            assertEquals(idSalary.get(1)[0], 2);
            assertEquals(idSalary.get(1)[1], 200L);
        }
    }

    @Test
    public void broadcastHashJoinEmitsInnerMatches() {
        // build side: department {deptId, deptName}; probe side: employee {empId, deptId}
        StreamLakeHashJoin join = new StreamLakeHashJoin(0); // build key = deptId (col 0)
        join.addBuildRow(new Object[]{10, "Eng"});
        join.addBuildRow(new Object[]{20, "Sales"});
        assertEquals(join.buildSize(), 2);

        List<Object[]> employees = Arrays.asList(
                new Object[]{1, 10}, new Object[]{2, 20}, new Object[]{3, 99}); // 99 has no dept
        List<Object[]> joined = join.joinInner(employees, 1); // probe key = deptId (col 1)

        assertEquals(joined.size(), 2, "only the two employees with a matching dept join");
        // concat(probe, build): [empId, deptId, deptId, deptName]
        assertEquals(joined.get(0)[0], 1);
        assertEquals(joined.get(0)[3], "Eng");
        assertEquals(joined.get(1)[3], "Sales");
    }

    @Test
    public void hashJoinAdmissionControlFailsFast() {
        StreamLakeHashJoin join = new StreamLakeHashJoin(0, 1);
        join.addBuildRow(new Object[]{1, "a"});
        try {
            join.addBuildRow(new Object[]{2, "b"});
            org.testng.Assert.fail("expected admission-control failure");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("build side exceeded"));
        }
    }

    @Test
    public void topKKeepsBestRowsDescendingWithPageSkip() {
        // ORDER BY salary DESC LIMIT 3
        StreamLakeTopK topK = new StreamLakeTopK(3, 1, true);
        int[] salaries = {100, 500, 300, 900, 200, 700};
        for (int s : salaries) {
            topK.offer(new Object[]{"e" + s, (long) s});
        }
        List<Object[]> top = topK.results();
        assertEquals(top.size(), 3);
        assertEquals(top.get(0)[1], 900L);
        assertEquals(top.get(1)[1], 700L);
        assertEquals(top.get(2)[1], 500L);

        // a page whose best (max) salary is 400 < current 3rd (500) can be skipped entirely.
        assertTrue(topK.canSkipPage(400L));
        // a page that could contain a 600 cannot be skipped.
        assertFalse(topK.canSkipPage(600L));
    }

    @Test
    public void topKAscendingKeepsSmallest() {
        StreamLakeTopK topK = new StreamLakeTopK(2, 0, false);
        topK.offerAll(Arrays.asList(
                new Object[]{50}, new Object[]{10}, new Object[]{30}, new Object[]{20}));
        List<Object[]> top = topK.results();
        assertEquals(top.size(), 2);
        assertEquals(top.get(0)[0], 10);
        assertEquals(top.get(1)[0], 20);
    }
}
