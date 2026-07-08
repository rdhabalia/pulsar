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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

/** Unit tests for the StreamLake stats primitives: order-preserving encoding and the bloom filter. */
public class StreamLakeStatsPrimitivesTest {

    @Test
    public void intEncodingIsOrderPreserving() {
        int[] vals = {Integer.MIN_VALUE, -1_000_000, -1, 0, 1, 1_000_000, Integer.MAX_VALUE};
        for (int i = 0; i < vals.length; i++) {
            for (int j = i + 1; j < vals.length; j++) {
                byte[] a = StreamLakeOrderPreserving.encInt(vals[i]);
                byte[] b = StreamLakeOrderPreserving.encInt(vals[j]);
                assertTrue(Arrays.compareUnsigned(a, b) < 0, vals[i] + " < " + vals[j]);
            }
        }
    }

    @Test
    public void longAndDoubleEncodingsAreOrderPreserving() {
        long[] longs = {Long.MIN_VALUE, -5L, 0L, 7L, Long.MAX_VALUE};
        for (int i = 0; i < longs.length; i++) {
            for (int j = i + 1; j < longs.length; j++) {
                assertTrue(Arrays.compareUnsigned(StreamLakeOrderPreserving.encLong(longs[i]),
                        StreamLakeOrderPreserving.encLong(longs[j])) < 0, longs[i] + " < " + longs[j]);
            }
        }
        double[] doubles = {-Double.MAX_VALUE, -3.5, -0.0, 0.0, 1e-9, 42.0, Double.MAX_VALUE};
        for (int i = 0; i < doubles.length; i++) {
            for (int j = i + 1; j < doubles.length; j++) {
                assertTrue(Arrays.compareUnsigned(StreamLakeOrderPreserving.encDouble(doubles[i]),
                        StreamLakeOrderPreserving.encDouble(doubles[j])) <= 0, doubles[i] + " <= " + doubles[j]);
            }
        }
    }

    @Test
    public void bloomHasNoFalseNegativesAndRoundTrips() {
        List<byte[]> members = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            members.add(("user-" + i + "@example.com").getBytes(StandardCharsets.UTF_8));
        }
        StreamLakeBloom bloom = StreamLakeBloom.build(members, 0.01);
        for (byte[] m : members) {
            assertTrue(bloom.mightContain(m), "member must be present");
        }
        StreamLakeBloom restored = StreamLakeBloom.decode(bloom.encode());
        for (byte[] m : members) {
            assertTrue(restored.mightContain(m), "member present after round-trip");
        }
        // A value never inserted should (very likely) be reported absent at 1% fpp.
        int falsePositives = 0;
        for (int i = 0; i < 1000; i++) {
            byte[] absent = ("absent-" + i).getBytes(StandardCharsets.UTF_8);
            if (restored.mightContain(absent)) {
                falsePositives++;
            }
        }
        assertFalse(falsePositives > 50, "false-positive rate should be near 1%, was " + falsePositives + "/1000");
    }
}
