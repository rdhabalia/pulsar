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
package org.apache.kafka.clients.consumer;

import java.util.Base64;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Queues;

import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import lombok.extern.slf4j.Slf4j;
import scala.Function1;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterable;
import scala.collection.generic.CanBuildFrom;

@Slf4j
public class PulsarKafkaStream2<K, V,That,B> {/*
    //extends kafka.consumer.KafkaStream<K,V> {

    private final Consumer<byte[]> consumer;

    private volatile boolean closed = false;

    private final Optional<Decoder<K>> keyDeSerializer;
    private final Optional<Decoder<V>> valueDeSerializer;
    
    private final ConcurrentLinkedQueue<Message<byte[]>> receivedMessages = Queues.newConcurrentLinkedQueue();

    public PulsarKafkaStream2(Decoder<K> keyDecoder, Decoder<V> valueDecoder, Consumer<byte[]> consumer) {
        super(null, 0, keyDecoder, valueDecoder, null);
        this.consumer = consumer;
        this.keyDeSerializer = Optional.ofNullable(keyDecoder);
        this.valueDeSerializer = Optional.ofNullable(valueDecoder);
        Function1<MessageAndMetadata<K,V>,B> f;
        CanBuildFrom<scala.collection.Iterable<MessageAndMetadata<K,V>>,B,That> bf;
    }
        

    @Override
    public Tuple3 unzip3(Function1 asTriple) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Tuple2 unzip(Function1 asPair) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ConsumerIterator<K, V> iterator() {
        String clientId;
        @SuppressWarnings({ "unchecked", "rawtypes" })
        ConsumerIterator<K, V> itr = new ConsumerIterator(null, 0, keyDeSerializer.orElse(null), valueDeSerializer.orElse(null), clientId) {
            @Override
            public boolean hasNext() {
                try {
                    Message<byte[]> msg = consumer.receive(10, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        receivedMessages.offer(msg);
                    }
                } catch (PulsarClientException e) {
                    //TODO: log and return false
                }
                return false;
            }

            @Override
            public PulsarMessageAndMetadata<K, V> next() {
                
                Message<byte[]> msg = receivedMessages.poll();
                if(msg==null) {
                    try {
                        msg = consumer.receive();
                    } catch (PulsarClientException e) {
                       //TODO: log and throw runtimeException
                    }
                }
                
                int partition = 0; //TODO : get partition from the topic name
                long offset = 0;
                String key = msg.getKey();
                byte[] value = msg.getValue();
                
                //TODO: remove key if not needed
                if (StringUtils.isNotBlank(key)) {
                    K desKey = null;
                    if (keyDeSerializer.isPresent() && keyDeSerializer.get() instanceof StringDecoder) {
                        desKey = (K) key;
                    } else {
                        byte[] decodedBytes = Base64.getDecoder().decode(key);
                        desKey = keyDeSerializer.isPresent() ? keyDeSerializer.get().fromBytes(decodedBytes)  :
                                SerializationUtils.deserialize(decodedBytes);
                    }
                }
                
                
                V desValue = null;
                if (value != null) {
                    desValue = valueDeSerializer.isPresent() ? valueDeSerializer.get().fromBytes(msg.getData()) :
                        SerializationUtils.deserialize(msg.getData());
                }

                PulsarMessageAndMetadata<K, V> msgAndMetadata = new PulsarMessageAndMetadata<>(consumer.getTopic(),
                        partition, null, offset, keyDeSerializer.orElse(null), valueDeSerializer.orElse(null), desValue);
                
                return msgAndMetadata;
            }
        };
        
        return null;
    }
    public Iterator<PulsarMessageAndMetadata<K, V>> iterator2() {
        return new Iterator<PulsarMessageAndMetadata<K, V>>() {

            @Override
            public boolean hasNext() {
                try {
                    Message<byte[]> msg = consumer.receive(10, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        receivedMessages.offer(msg);
                    }
                } catch (PulsarClientException e) {
                    //TODO: log and return false
                }
                return false;
            }

            @Override
            public PulsarMessageAndMetadata<K, V> next() {
                
                Message<byte[]> msg = receivedMessages.poll();
                if(msg==null) {
                    try {
                        msg = consumer.receive();
                    } catch (PulsarClientException e) {
                       //TODO: log and throw runtimeException
                    }
                }
                
                int partition = 0; //TODO : get partition from the topic name
                long offset = 0;
                String key = msg.getKey();
                byte[] value = msg.getValue();
                
                //TODO: remove key if not needed
                if (StringUtils.isNotBlank(key)) {
                    K desKey = null;
                    if (keyDeSerializer.isPresent() && keyDeSerializer.get() instanceof StringDecoder) {
                        desKey = (K) key;
                    } else {
                        byte[] decodedBytes = Base64.getDecoder().decode(key);
                        desKey = keyDeSerializer.isPresent() ? keyDeSerializer.get().fromBytes(decodedBytes)  :
                                SerializationUtils.deserialize(decodedBytes);
                    }
                }
                
                
                V desValue = null;
                if (value != null) {
                    desValue = valueDeSerializer.isPresent() ? valueDeSerializer.get().fromBytes(msg.getData()) :
                        SerializationUtils.deserialize(msg.getData());
                }

                PulsarMessageAndMetadata<K, V> msgAndMetadata = new PulsarMessageAndMetadata<>(consumer.getTopic(),
                        partition, null, offset, keyDeSerializer.orElse(null), valueDeSerializer.orElse(null), desValue);
                
                return msgAndMetadata;
            }

        };
    }
*/}
