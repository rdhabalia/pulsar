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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.testng.annotations.Test;

/** Tests decoding a StreamLake columnar message into records and per-record acknowledgement. */
public class StreamLakeConsumerTest {

    private static StreamLakeTopicSchema topicSchema() {
        StreamingLakeConfig config = StreamingLakeConfig.builder()
                .enabled(true).setMaxCardinality(64).bloomFpp(0.01)
                .columns(Arrays.asList(
                        new StreamingLakeConfig.SchemaColumn(1, "id", "INT32", true),
                        new StreamingLakeConfig.SchemaColumn(2, "email", "STRING", true)))
                .build();
        return StreamLakeTopicSchema.fromConfig(config);
    }

    private static byte[] payload(StreamLakeTopicSchema topic, List<Object[]> rows) {
        byte[] arrow;
        try (StreamLakeArrowBatchEncoder enc = new StreamLakeArrowBatchEncoder(topic.schema())) {
            arrow = enc.encode(rows);
        }
        byte[] footer = StreamLakeStatsBuilder.build(topic.schema(), rows, topic.indexedColumns(),
                topic.setMaxCardinality(), topic.bloomFpp()).encode();
        return StreamLakeBatchPayload.combine(arrow, footer);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void decodesRecordsAndAcksMessageWhenAllRecordsAcked() throws Exception {
        StreamLakeTopicSchema topic = topicSchema();
        List<Object[]> input = new ArrayList<>();
        input.add(new Object[]{1, "a@x.com"});
        input.add(new Object[]{2, "b@x.com"});
        input.add(new Object[]{3, "c@x.com"});
        byte[] payload = payload(topic, input);

        Consumer<byte[]> consumer = mock(Consumer.class);
        Message<byte[]> message = mock(Message.class);
        MessageId id = mock(MessageId.class);
        when(message.getValue()).thenReturn(payload);
        when(message.getMessageId()).thenReturn(id);
        when(consumer.receive()).thenReturn(message);
        when(consumer.acknowledgeAsync(id)).thenReturn(CompletableFuture.completedFuture(null));

        try (StreamLakeConsumer slc = new StreamLakeConsumer(consumer)) {
            StreamLakeRecordBatch batch = slc.receiveBatch();
            assertEquals(batch.size(), 3);
            assertEquals(batch.row(0)[0], 1);
            assertEquals(batch.row(2)[1], "c@x.com");
            assertNotNull(batch.statsFooter());

            batch.ackRecord(0).get();
            batch.ackRecord(1).get();
            verify(consumer, never()).acknowledgeAsync(any(MessageId.class));

            batch.ackRecord(2).get();
            verify(consumer, times(1)).acknowledgeAsync(id);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void ackAllAcksTheMessageOnce() throws Exception {
        StreamLakeTopicSchema topic = topicSchema();
        List<Object[]> input = new ArrayList<>();
        input.add(new Object[]{7, "z@x.com"});
        byte[] payload = payload(topic, input);

        Consumer<byte[]> consumer = mock(Consumer.class);
        Message<byte[]> message = mock(Message.class);
        MessageId id = mock(MessageId.class);
        when(message.getValue()).thenReturn(payload);
        when(message.getMessageId()).thenReturn(id);
        when(consumer.receive()).thenReturn(message);
        when(consumer.acknowledgeAsync(id)).thenReturn(CompletableFuture.completedFuture(null));

        try (StreamLakeConsumer slc = new StreamLakeConsumer(consumer)) {
            StreamLakeRecordBatch batch = slc.receiveBatch();
            assertEquals(batch.size(), 1);
            batch.ackAll().get();
            verify(consumer, times(1)).acknowledgeAsync(id);
        }
    }
}
