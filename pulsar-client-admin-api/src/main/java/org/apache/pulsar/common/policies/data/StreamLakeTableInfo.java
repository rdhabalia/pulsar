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
package org.apache.pulsar.common.policies.data;

import java.util.ArrayList;
import java.util.List;

/**
 * A snapshot of a StreamLake table's on-storage layout (returned by {@code pulsar-admin streamlake
 * info}): the data-ledger inventory (total / segmented / open), the metadata ledgers that back it
 * (catalog, page-index chain, segment chain — the only things kept in ZooKeeper), the data-page
 * (entry) count and the event-time range. Lets an operator confirm, after ingestion, exactly how many
 * ledgers of each tier were created. (Logical rows = {@code dataPages * rowsPerPage}; the exact row
 * count is echoed by the ingestion program.)
 */
public class StreamLakeTableInfo {

    private String topic;
    private long dataLedgers;
    private long dataLedgersSegmented;
    private long dataLedgersOpen;
    private Long catalogLedgerId;
    private List<Long> pageIndexLedgerIds = new ArrayList<>();
    private List<Long> segmentLedgerIds = new ArrayList<>();
    private long dataPages;
    private long minEventTime;
    private long maxEventTime;

    public StreamLakeTableInfo() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getDataLedgers() {
        return dataLedgers;
    }

    public void setDataLedgers(long dataLedgers) {
        this.dataLedgers = dataLedgers;
    }

    public long getDataLedgersSegmented() {
        return dataLedgersSegmented;
    }

    public void setDataLedgersSegmented(long dataLedgersSegmented) {
        this.dataLedgersSegmented = dataLedgersSegmented;
    }

    public long getDataLedgersOpen() {
        return dataLedgersOpen;
    }

    public void setDataLedgersOpen(long dataLedgersOpen) {
        this.dataLedgersOpen = dataLedgersOpen;
    }

    public Long getCatalogLedgerId() {
        return catalogLedgerId;
    }

    public void setCatalogLedgerId(Long catalogLedgerId) {
        this.catalogLedgerId = catalogLedgerId;
    }

    public List<Long> getPageIndexLedgerIds() {
        return pageIndexLedgerIds;
    }

    public void setPageIndexLedgerIds(List<Long> pageIndexLedgerIds) {
        this.pageIndexLedgerIds = pageIndexLedgerIds;
    }

    public List<Long> getSegmentLedgerIds() {
        return segmentLedgerIds;
    }

    public void setSegmentLedgerIds(List<Long> segmentLedgerIds) {
        this.segmentLedgerIds = segmentLedgerIds;
    }

    /** Number of shared page-index ledgers (footer chain). */
    public int getPageIndexLedgers() {
        return pageIndexLedgerIds == null ? 0 : pageIndexLedgerIds.size();
    }

    /** Number of segment index-ledgers. */
    public int getSegmentLedgers() {
        return segmentLedgerIds == null ? 0 : segmentLedgerIds.size();
    }

    public long getDataPages() {
        return dataPages;
    }

    public void setDataPages(long dataPages) {
        this.dataPages = dataPages;
    }

    public long getMinEventTime() {
        return minEventTime;
    }

    public void setMinEventTime(long minEventTime) {
        this.minEventTime = minEventTime;
    }

    public long getMaxEventTime() {
        return maxEventTime;
    }

    public void setMaxEventTime(long maxEventTime) {
        this.maxEventTime = maxEventTime;
    }
}
