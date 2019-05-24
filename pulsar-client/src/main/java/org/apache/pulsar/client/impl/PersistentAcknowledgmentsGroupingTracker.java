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
package org.apache.pulsar.client.impl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.ValidationError;

/**
 * Group the acknowledgements for a certain time and then sends them out in a single protobuf command.
 */
@Slf4j
public class PersistentAcknowledgmentsGroupingTracker implements AcknowledgmentsGroupingTracker {

    /**
     * When reaching the max group size, an ack command is sent out immediately
     */
    private static final int MAX_ACK_GROUP_SIZE = 1000;

    private final ConsumerImpl<?> consumer;

    private final long acknowledgementGroupTimeMicros;

    /**
     * Latest cumulative ack sent to broker
     */
    private volatile MessageIdImpl lastCumulativeAck = (MessageIdImpl) MessageId.earliest;
    private volatile boolean cumulativeAckFlushRequired = false;

    private static final AtomicReferenceFieldUpdater<PersistentAcknowledgmentsGroupingTracker, MessageIdImpl> LAST_CUMULATIVE_ACK_UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(PersistentAcknowledgmentsGroupingTracker.class, MessageIdImpl.class, "lastCumulativeAck");

    /**
     * This is a set of all the individual acks that the application has issued and that were not already sent to
     * broker.
     */
    private final ConcurrentSkipListSet<MessageIdImpl> pendingIndividualAcks;

    private final ScheduledFuture<?> scheduledTask;

    public PersistentAcknowledgmentsGroupingTracker(ConsumerImpl<?> consumer, ConsumerConfigurationData<?> conf,
                                                    EventLoopGroup eventLoopGroup) {
        this.consumer = consumer;
        this.pendingIndividualAcks = new ConcurrentSkipListSet<>();
        this.acknowledgementGroupTimeMicros = conf.getAcknowledgementsGroupTimeMicros();

        if (acknowledgementGroupTimeMicros > 0) {
            scheduledTask = eventLoopGroup.next().scheduleWithFixedDelay(this::flush, acknowledgementGroupTimeMicros,
                    acknowledgementGroupTimeMicros, TimeUnit.MICROSECONDS);
        } else {
            scheduledTask = null;
        }
    }

    /**
     * Since the ack are delayed, we need to do some best-effort duplicate check to discard messages that are being
     * resent after a disconnection and for which the user has already sent an acknowledgement.
     */
    public boolean isDuplicate(MessageId messageId) {
        if (messageId.compareTo(lastCumulativeAck) <= 0) {
            // Already included in a cumulative ack
            return true;
        } else {
            return pendingIndividualAcks.contains(messageId);
        }
    }

    public void addAcknowledgment(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties) {
        if (acknowledgementGroupTimeMicros == 0 || !properties.isEmpty()) {
            // We cannot group acks if the delay is 0 or when there are properties attached to it. Fortunately that's an
            // uncommon condition since it's only used for the compaction subscription.
            doImmediateAck(msgId, ackType, properties);
        } else if (ackType == AckType.Cumulative) {
            doCumulativeAck(msgId);
        } else {
            // Individual ack
            pendingIndividualAcks.add(msgId);
            if (pendingIndividualAcks.size() >= MAX_ACK_GROUP_SIZE) {
                flush();
            }
        }
    }

    private void doCumulativeAck(MessageIdImpl msgId) {
        // Handle concurrent updates from different threads
        while (true) {
            MessageIdImpl lastCumlativeAck = this.lastCumulativeAck;
            if (msgId.compareTo(lastCumlativeAck) > 0) {
                if (LAST_CUMULATIVE_ACK_UPDATER.compareAndSet(this, lastCumlativeAck, msgId)) {
                    // Successfully updated the last cumulative ack. Next flush iteration will send this to broker.
                    cumulativeAckFlushRequired = true;
                    return;
                }
            } else {
                // message id acknowledging an before the current last cumulative ack
                return;
            }
        }
    }

    private boolean doImmediateAck(MessageIdImpl msgId, AckType ackType, Map<String, Long> properties) {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            return false;
        }

        newAckCommand(consumer.consumerId, msgId, ackType, null, properties, cnx, true /* flush */);

        return true;
    }

    /**
     * Flush all the pending acks and send them to the broker
     */
    public void flush() {
        ClientCnx cnx = consumer.getClientCnx();

        if (cnx == null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cannot flush pending acks since we're not connected to broker", consumer);
            }
            return;
        }

        boolean shouldFlush = false;
        if (cumulativeAckFlushRequired) {
            newAckCommand(consumer.consumerId, lastCumulativeAck, AckType.Cumulative, null, Collections.emptyMap(), cnx, false /* flush */);
            shouldFlush=true;
            cumulativeAckFlushRequired = false;
        }

        // Flush all individual acks
        if (!pendingIndividualAcks.isEmpty()) {
            if (Commands.peerSupportsMultiMessageAcknowledgment(cnx.getRemoteEndpointProtocolVersion())) {
                // We can send 1 single protobuf command with all individual acks
                List<Pair<Long, Long>> entriesToAck = new ArrayList<>(pendingIndividualAcks.size());
                while (true) {
                    MessageIdImpl msgId = pendingIndividualAcks.pollFirst();
                    if (msgId == null) {
                        break;
                    }

                    // if messageId is checked then all the chunked related to that msg also processed so, ack all of
                    // them 
                    MessageIdImpl[] chunkMsgIds = this.consumer.chunckedMessageIdSequenceMap.get(msgId);
                    if (chunkMsgIds != null && chunkMsgIds.length > 1) {
                        for (MessageIdImpl cMsgId : chunkMsgIds) {
                            if (cMsgId != null) {
                                entriesToAck.add(Pair.of(cMsgId.getLedgerId(), cMsgId.getEntryId()));
                            }
                        }
                        // messages will be acked so, remove checked message sequence
                        this.consumer.chunckedMessageIdSequenceMap.remove(msgId);
                    } else {
                        entriesToAck.add(Pair.of(msgId.getLedgerId(), msgId.getEntryId()));
                    }
                }
                cnx.ctx().write(Commands.newMultiMessageAck(consumer.consumerId, entriesToAck),
                        cnx.ctx().voidPromise());
                shouldFlush = true;
            } else {
                // When talking to older brokers, send the acknowledgements individually
                while (true) {
                    MessageIdImpl msgId = pendingIndividualAcks.pollFirst();
                    if (msgId == null) {
                        break;
                    }

                    newAckCommand(consumer.consumerId, msgId, AckType.Individual, null, Collections.emptyMap(),cnx,  false);

                    shouldFlush = true;
                }
            }
        }

        if (shouldFlush) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Flushing pending acks to broker: last-cumulative-ack: {} -- individual-acks: {}",
                        consumer, lastCumulativeAck, pendingIndividualAcks);
            }
            cnx.ctx().flush();
        }
    }

    private void newAckCommand(long consumerId, MessageIdImpl msgId, AckType ackType,
            ValidationError validationError, Map<String, Long> map, ClientCnx cnx, boolean flush) {

        MessageIdImpl[] chunkMsgIds = this.consumer.chunckedMessageIdSequenceMap.get(msgId);
        if (chunkMsgIds != null) {
            if (Commands.peerSupportsMultiMessageAcknowledgment(cnx.getRemoteEndpointProtocolVersion()) && ackType != AckType.Cumulative) {
                List<Pair<Long, Long>> entriesToAck = new ArrayList<>(chunkMsgIds.length);
                for (MessageIdImpl cMsgId : chunkMsgIds) {
                    if (cMsgId != null && chunkMsgIds.length > 1) {
                        entriesToAck.add(Pair.of(cMsgId.getLedgerId(), cMsgId.getEntryId()));
                    }
                }
                ByteBuf cmd = Commands.newMultiMessageAck(consumer.consumerId, entriesToAck);
                if (flush) {
                    cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
                } else {
                    cnx.ctx().write(cmd, cnx.ctx().voidPromise());
                }
            } else {
                for (MessageIdImpl cMsgId : chunkMsgIds) {
                    ByteBuf cmd = Commands.newAck(consumerId, cMsgId.getLedgerId(), cMsgId.getEntryId(), ackType,
                            validationError, map);
                    if (flush) {
                        cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
                    } else {
                        cnx.ctx().write(cmd, cnx.ctx().voidPromise());
                    }
                }
            }
            this.consumer.chunckedMessageIdSequenceMap.remove(msgId);
        } else {
            ByteBuf cmd = Commands.newAck(consumerId, msgId.getLedgerId(), msgId.getEntryId(), ackType, validationError,
                    map);
            if (flush) {
                cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
            } else {
                cnx.ctx().write(cmd, cnx.ctx().voidPromise());
            }
        }
    }
    
    @Override
    public void flushAndClean() {
        flush();
        lastCumulativeAck = (MessageIdImpl) MessageId.earliest;
        pendingIndividualAcks.clear();
    }

    @Override
    public void close() {
        flush();
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
        }
    }
}
