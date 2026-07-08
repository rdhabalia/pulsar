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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

/** Tests that the producer accumulates rows and flushes them as a single StreamLake columnar payload. */
public class StreamLakeProducerTest {

    private static StreamLakeTopicSchema topicSchema() {
        StreamingLakeConfig config = StreamingLakeConfig.builder()
                .enabled(true).setMaxCardinality(64).bloomFpp(0.01)
                .columns(Arrays.asList(
                        new StreamingLakeConfig.SchemaColumn(1, "id", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(2, "deptId", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(3, "email", "STRING", true)))
                .build();
        return StreamLakeTopicSchema.fromConfig(config);
    }

    @SuppressWarnings("unchecked")
    private static Producer<byte[]> mockProducer(ArgumentCaptor<byte[]> captor) {
        Producer<byte[]> producer = mock(Producer.class);
        when(producer.sendAsync(captor.capture()))
                .thenReturn(CompletableFuture.completedFuture(mock(MessageId.class)));
        return producer;
    }

    @Test
    public void flushesColumnarPayloadOnMaxRows() throws Exception {
        ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
        Producer<byte[]> producer = mockProducer(captor);
        try (StreamLakeProducer p = new StreamLakeProducer(producer, topicSchema(), 3, Long.MAX_VALUE, 0)) {
            p.addRow(new Object[]{1, 10, "a@x.com"});
            p.addRow(new Object[]{2, 20, "b@x.com"});
            p.addRow(new Object[]{3, 30, "c@x.com"}); // hits maxRows=3 -> flush
        }
        List<byte[]> sent = captor.getAllValues();
        assertEquals(sent.size(), 1);
        byte[] payload = sent.get(0);
        assertTrue(StreamLakeBatchPayload.hasFooter(payload));

        List<Object[]> rows;
        try (StreamLakeArrowBatchDecoder decoder = new StreamLakeArrowBatchDecoder()) {
            rows = decoder.decodeRows(StreamLakeBatchPayload.arrowBatch(payload));
        }
        assertEquals(rows.size(), 3);
        assertEquals(rows.get(1)[0], 2);
        assertEquals(rows.get(2)[2], "c@x.com");

        StreamLakeBatchStats stats = StreamLakeBatchStats.decode(StreamLakeBatchPayload.statsFooter(payload));
        assertNotNull(stats.column(0), "id indexed");
        assertNotNull(stats.column(2), "email indexed");
    }

    @Test
    public void closeFlushesRemainingRows() throws Exception {
        ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
        Producer<byte[]> producer = mockProducer(captor);
        try (StreamLakeProducer p = new StreamLakeProducer(producer, topicSchema(), 1000, Long.MAX_VALUE, 0)) {
            p.addRow(new Object[]{1, 10, "a@x.com"});
            p.addRow(new Object[]{2, 20, "b@x.com"});
            // below maxRows; close() must flush the remainder
        }
        List<byte[]> sent = captor.getAllValues();
        assertEquals(sent.size(), 1);
        try (StreamLakeArrowBatchDecoder decoder = new StreamLakeArrowBatchDecoder()) {
            assertEquals(decoder.decodeRows(StreamLakeBatchPayload.arrowBatch(sent.get(0))).size(), 2);
        }
    }

    @Test
    public void explicitFlushSends() throws Exception {
        ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
        Producer<byte[]> producer = mockProducer(captor);
        try (StreamLakeProducer p = new StreamLakeProducer(producer, topicSchema(), 1000, Long.MAX_VALUE, 0)) {
            p.addRow(new Object[]{1, 10, "a@x.com"});
            p.flush().get();
            assertEquals(captor.getAllValues().size(), 1);
        }
    }
}
