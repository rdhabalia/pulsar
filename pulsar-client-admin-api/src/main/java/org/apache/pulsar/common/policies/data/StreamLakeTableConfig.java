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
 * The request body for {@code pulsar-admin streamlake register}: a lean, dependency-free description of
 * how to turn a topic into a StreamLake table (schema + tuning). The broker converts it into the
 * internal {@code StreamingLakeConfig} topic policy. Kept in the admin-api module (which does not depend
 * on pulsar-common) so both the CLI and the REST layer can share it.
 */
public class StreamLakeTableConfig {

    /** One schema column: a stable id, name, StreamLake type name, and whether it emits pruning stats. */
    public static class Column {
        private int columnId;
        private String name;
        private String type;
        private boolean indexed = true;

        public Column() {
        }

        public Column(int columnId, String name, String type, boolean indexed) {
            this.columnId = columnId;
            this.name = name;
            this.type = type;
            this.indexed = indexed;
        }

        public int getColumnId() {
            return columnId;
        }

        public void setColumnId(int columnId) {
            this.columnId = columnId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public boolean isIndexed() {
            return indexed;
        }

        public void setIndexed(boolean indexed) {
            this.indexed = indexed;
        }
    }

    private List<Column> columns = new ArrayList<>();
    private int setMaxCardinality = 64;
    private double bloomFpp = 0.01;
    private int pageIndexMaxEntriesPerLedger = 1_000_000;
    private int segmentMaxEntriesPerLedger = 200_000;
    private int replicationFactor = 1;
    private boolean asyncSegmentBuildViaSystemTopic;
    private String joinSpillDir = "";

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public int getSetMaxCardinality() {
        return setMaxCardinality;
    }

    public void setSetMaxCardinality(int setMaxCardinality) {
        this.setMaxCardinality = setMaxCardinality;
    }

    public double getBloomFpp() {
        return bloomFpp;
    }

    public void setBloomFpp(double bloomFpp) {
        this.bloomFpp = bloomFpp;
    }

    public int getPageIndexMaxEntriesPerLedger() {
        return pageIndexMaxEntriesPerLedger;
    }

    public void setPageIndexMaxEntriesPerLedger(int pageIndexMaxEntriesPerLedger) {
        this.pageIndexMaxEntriesPerLedger = pageIndexMaxEntriesPerLedger;
    }

    public int getSegmentMaxEntriesPerLedger() {
        return segmentMaxEntriesPerLedger;
    }

    public void setSegmentMaxEntriesPerLedger(int segmentMaxEntriesPerLedger) {
        this.segmentMaxEntriesPerLedger = segmentMaxEntriesPerLedger;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public boolean isAsyncSegmentBuildViaSystemTopic() {
        return asyncSegmentBuildViaSystemTopic;
    }

    public void setAsyncSegmentBuildViaSystemTopic(boolean asyncSegmentBuildViaSystemTopic) {
        this.asyncSegmentBuildViaSystemTopic = asyncSegmentBuildViaSystemTopic;
    }

    /**
     * Broker-local directory for hash-join spill/partition files. Empty (default) uses the broker JVM
     * temp dir (typically {@code /tmp}); point it at a large/fast local disk so big joins do not fill a
     * small {@code /tmp}. Files are deleted after the join completes.
     */
    public String getJoinSpillDir() {
        return joinSpillDir;
    }

    public void setJoinSpillDir(String joinSpillDir) {
        this.joinSpillDir = joinSpillDir;
    }
}
