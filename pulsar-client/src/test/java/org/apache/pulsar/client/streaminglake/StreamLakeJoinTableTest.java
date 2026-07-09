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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.util.List;
import org.testng.annotations.Test;

/** Row codec round-trip and the two {@link StreamLakeJoinTable} backends (on-heap + disk-spilling). */
public class StreamLakeJoinTableTest {

    @Test
    public void rowCodecRoundTripsAllTypes() {
        Object[] row = {42, 9_000_000_000L, 3.5d, true, "café", new byte[]{1, 2, 3}, null};
        Object[] out = StreamLakeRowCodec.decode(StreamLakeRowCodec.encode(row));
        assertEquals(out[0], 42);
        assertEquals(out[1], 9_000_000_000L);
        assertEquals(out[2], 3.5d);
        assertEquals(out[3], true);
        assertEquals(out[4], "café");
        assertEquals((byte[]) out[5], new byte[]{1, 2, 3});
        assertNull(out[6]);
    }

    @Test
    public void onHeapTableStoresAndLooksUp() {
        try (StreamLakeJoinTable t = new OnHeapJoinTable(Long.MAX_VALUE)) {
            t.add(1, new Object[]{1, "a"});
            t.add(1, new Object[]{1, "b"}); // multi-value key
            t.add(2, new Object[]{2, "c"});
            assertEquals(t.size(), 3);
            assertEquals(t.get(1).size(), 2);
            assertEquals(t.get(2).get(0)[1], "c");
            assertTrue(t.get(99).isEmpty());
        }
    }

    @Test
    public void onHeapTableFailsFastPastMaxRows() {
        try (StreamLakeJoinTable t = new OnHeapJoinTable(1)) {
            t.add(1, new Object[]{1});
            assertThrows(IllegalStateException.class, () -> t.add(2, new Object[]{2}));
        }
    }

    @Test
    public void spillingTableRoundTripsThroughDisk() {
        try (StreamLakeJoinTable t = new SpillingJoinTable(Long.MAX_VALUE, "")) {
            t.add(7, new Object[]{7, "seven", 700L});
            t.add(7, new Object[]{7, "siete", 701L});   // multi-value key
            t.add(8, new Object[]{8, null, new byte[]{9}});
            assertEquals(t.size(), 3);

            List<Object[]> seven = t.get(7);
            assertEquals(seven.size(), 2);
            assertEquals(seven.get(0)[1], "seven");
            assertEquals(seven.get(1)[2], 701L);

            List<Object[]> eight = t.get(8);
            assertEquals(eight.size(), 1);
            assertNull(eight.get(0)[1]);
            assertEquals((byte[]) eight.get(0)[2], new byte[]{9});

            assertTrue(t.get(404).isEmpty());
        }
    }

    @Test
    public void bothBackendsAgree() {
        Object[][] rows = {
                {1, "a", 10L}, {1, "b", 11L}, {2, "c", 12L}, {3, "d", 13L}};
        try (StreamLakeJoinTable heap = new OnHeapJoinTable(Long.MAX_VALUE);
             StreamLakeJoinTable spill = new SpillingJoinTable(Long.MAX_VALUE, "")) {
            for (Object[] r : rows) {
                heap.add(r[0], r);
                spill.add(r[0], r);
            }
            for (Object key : new Object[]{1, 2, 3, 99}) {
                assertEquals(spill.get(key).size(), heap.get(key).size(), "key " + key);
            }
            assertEquals(spill.get(1).get(0)[1], heap.get(1).get(0)[1]);
        }
    }
}
