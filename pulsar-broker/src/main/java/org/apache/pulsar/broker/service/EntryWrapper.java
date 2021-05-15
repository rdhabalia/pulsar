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
package org.apache.pulsar.broker.service;

import io.netty.util.Recycler;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.api.proto.MessageMetadata;

public class EntryWrapper {
    private Entry entry = null;
    private boolean metadataPresent=false;
    private int markerType;
    private long txnidMostBits;
    private long txnidLeastBits;
    private long deliverAtTime;
    private static final long NON_EXIST_VALUE = -1;

    public static EntryWrapper get(Entry entry, MessageMetadata metadata) {
        EntryWrapper entryWrapper = RECYCLER.get();
        entryWrapper.entry = entry;
        if (metadata != null) {
            entryWrapper.metadataPresent = true;
            entryWrapper.markerType = metadata.hasMarkerType() ? metadata.getMarkerType() : (int) NON_EXIST_VALUE;
            entryWrapper.txnidMostBits = metadata.hasTxnidMostBits() ? metadata.getTxnidMostBits() : NON_EXIST_VALUE;
            entryWrapper.txnidLeastBits = metadata.hasTxnidLeastBits() ? metadata.getTxnidLeastBits() : NON_EXIST_VALUE;
            entryWrapper.deliverAtTime = metadata.hasDeliverAtTime() ? metadata.getDeliverAtTime() : NON_EXIST_VALUE;
        }
        return entryWrapper;
    }

    private EntryWrapper(Recycler.Handle<EntryWrapper> handle) {
        this.handle = handle;
    }

    public Entry getEntry() {
        return entry;
    }

    public boolean isMetadataPresent() {
        return metadataPresent;
    }

    private final Recycler.Handle<EntryWrapper> handle;
    private static final Recycler<EntryWrapper> RECYCLER = new Recycler<EntryWrapper>() {
        @Override
        protected EntryWrapper newObject(Handle<EntryWrapper> handle) {
            return new EntryWrapper(handle);
        }
    };

    public int getMarkerType() {
        return markerType;
    }

    public boolean hasMarkerType() {
        return this.markerType != NON_EXIST_VALUE;
    }

    public long getTxnidMostBits() {
        return txnidMostBits;
    }

    public boolean hasTxnidMostBits() {
        return this.txnidMostBits != NON_EXIST_VALUE;
    }

    public long getTxnidLeastBits() {
        return txnidLeastBits;
    }

    public boolean hasTxnidLeastBits() {
        return this.txnidLeastBits != NON_EXIST_VALUE;
    }

    public long getDeliverAtTime() {
        return deliverAtTime;
    }

    public void recycle() {
        entry = null;
        metadataPresent = false;
        txnidMostBits = NON_EXIST_VALUE;
        txnidLeastBits= NON_EXIST_VALUE;
        deliverAtTime=NON_EXIST_VALUE;
        handle.recycle(this);
    }
}