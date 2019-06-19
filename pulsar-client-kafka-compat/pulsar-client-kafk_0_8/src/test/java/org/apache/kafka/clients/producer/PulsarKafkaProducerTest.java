/**
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
package org.apache.kafka.clients.producer;

import java.util.Properties;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import kafka.producer.ProducerConfig;

//@PrepareForTest({PulsarClientKafkaConfig.class})
//@PowerMockIgnore({"org.apache.logging.log4j.*", "org.apache.kafka.clients.producer.ProducerInterceptor"})
public class PulsarKafkaProducerTest {

    private static final String BROKER_URL = "metadata.broker.list";
    private static final String PRODUCER_TYPE = "producer.type";
    private static final String SERIALIZER_CLASS = "serializer.class";
    private static final String KEY_SERIALIZER_CLASS = "key.serializer.class";
    private static final String PARTITIONER_CLASS = "partitioner.class";
    private static final String COMPRESSION_CODEC = "compression.codec";
    private static final String QUEUE_BUFFERING_MAX_MS = "queue.buffering.max.ms";
    private static final String QUEUE_BUFFERING_MAX_MESSAGES = "queue.buffering.max.messages";
    private static final String QUEUE_ENQUEUE_TIMEOUT_MS     = "queue.enqueue.timeout.ms";
    private static final String BATCH_NUM_MESSAGES = "batch.num.messages";
    private static final String CLIENT_ID = "client.id";
    

    /*@ObjectFactory
    // Necessary to make PowerMockito.mockStatic work with TestNG.
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }*/

    @Test
    public void testPulsarKafkaProducer() {
        // https://kafka.apache.org/08/documentation.html#producerconfigs
        Properties properties = new Properties();
        properties.put(BROKER_URL, "http://localhost:8080/");
        properties.put(PRODUCER_TYPE, "sync");
        properties.put(SERIALIZER_CLASS, "kafka.serializer.StringEncoder");
        properties.put(KEY_SERIALIZER_CLASS, "kafka.serializer.StringEncoder");
        /*properties.put(PARTITIONER_CLASS, "test");
        properties.put(COMPRESSION_CODEC, "test");
        properties.put(QUEUE_BUFFERING_MAX_MS, "test");
        properties.put(QUEUE_BUFFERING_MAX_MESSAGES, "test");
        properties.put(QUEUE_ENQUEUE_TIMEOUT_MS, "test");*/
        properties.put(BATCH_NUM_MESSAGES, "2000");
        properties.put(CLIENT_ID, "test");
        ProducerConfig config = new ProducerConfig(properties);
        PulsarKafkaProducer<byte[], byte[]> producer = new PulsarKafkaProducer<>(config);
    }
    
   
}
