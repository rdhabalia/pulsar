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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.testng.annotations.Test;

/** Bridges a topic's StreamingLakeConfig into the client encode/stats view and drives it end to end. */
public class StreamLakeTopicSchemaTest {

    private static StreamingLakeConfig config() {
        return StreamingLakeConfig.builder()
                .enabled(true)
                .setMaxCardinality(64)
                .bloomFpp(0.01)
                .columns(Arrays.asList(
                        new StreamingLakeConfig.SchemaColumn(1, "id", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(2, "name", "STRING", false),
                        new StreamingLakeConfig.SchemaColumn(3, "deptId", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(4, "email", "STRING", true)))
                .build();
    }

    @Test
    public void derivesSchemaAndIndexedColumnsFromConfig() {
        StreamLakeTopicSchema topic = StreamLakeTopicSchema.fromConfig(config());
        assertEquals(topic.schema().size(), 4);
        assertEquals(topic.schema().columns().get(0).name(), "id");
        assertEquals(topic.schema().columns().get(0).type(), StreamLakeType.INT32);
        assertEquals(topic.schema().columns().get(3).type(), StreamLakeType.STRING);
        assertEquals(topic.indexedColumns(), Arrays.asList(0, 2, 3)); // 'name' (index 1) not indexed
        assertEquals((long) topic.setMaxCardinality(), 64L);
        assertEquals(topic.bloomFpp(), 0.01, 1e-9);
    }

    @Test
    public void endToEndConfigToEncodedBatchAndStats() {
        StreamLakeTopicSchema topic = StreamLakeTopicSchema.fromConfig(config());
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(new Object[]{i, "n" + i, i % 4, "u" + i + "@x.com"});
        }

        byte[] ipc;
        try (StreamLakeArrowBatchEncoder encoder = new StreamLakeArrowBatchEncoder(topic.schema())) {
            ipc = encoder.encode(rows);
        }
        List<Object[]> out;
        try (StreamLakeArrowBatchDecoder decoder = new StreamLakeArrowBatchDecoder()) {
            out = decoder.decodeRows(ipc);
        }
        assertEquals(out.size(), 100);
        assertEquals(out.get(7)[0], 7);
        assertEquals(out.get(7)[3], "u7@x.com");

        StreamLakeBatchStats stats = StreamLakeStatsBuilder.build(topic.schema(), rows,
                topic.indexedColumns(), topic.setMaxCardinality(), topic.bloomFpp());
        assertNotNull(stats.column(0), "id is indexed");
        assertNull(stats.column(1), "name is not indexed");
        assertNotNull(stats.column(2).exactSet, "deptId (4 distinct) -> exact set");
        assertNotNull(stats.column(3).bloom, "email (100 distinct) -> bloom");
    }
}
