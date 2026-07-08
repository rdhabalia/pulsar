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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;

/**
 * The client-side view of a topic's StreamLake registration: the ordered {@link StreamLakeSchema} to
 * encode, which columns are indexed (emit per-batch pruning stats), and the cardinality / bloom
 * knobs. Derived from the topic's {@link StreamingLakeConfig} (a topic policy), so a producer or
 * consumer can drive the encoder/decoder and stats builder straight from the registered config.
 */
public final class StreamLakeTopicSchema {

    private final StreamLakeSchema schema;
    private final List<Integer> indexedColumns;
    private final int setMaxCardinality;
    private final double bloomFpp;

    public StreamLakeTopicSchema(StreamLakeSchema schema, List<Integer> indexedColumns,
            int setMaxCardinality, double bloomFpp) {
        this.schema = schema;
        this.indexedColumns = Collections.unmodifiableList(new ArrayList<>(indexedColumns));
        this.setMaxCardinality = setMaxCardinality;
        this.bloomFpp = bloomFpp;
    }

    public StreamLakeSchema schema() {
        return schema;
    }

    /** Indexes (into {@link #schema()}) of the columns that emit per-batch pruning stats. */
    public List<Integer> indexedColumns() {
        return indexedColumns;
    }

    public int setMaxCardinality() {
        return setMaxCardinality;
    }

    public double bloomFpp() {
        return bloomFpp;
    }

    /** Derive the client encode/stats view from a topic's StreamLake config (column order preserved). */
    public static StreamLakeTopicSchema fromConfig(StreamingLakeConfig config) {
        List<StreamingLakeConfig.SchemaColumn> cols = config.getColumns();
        List<StreamLakeSchema.Column> schemaCols = new ArrayList<>(cols.size());
        List<Integer> indexed = new ArrayList<>();
        for (int i = 0; i < cols.size(); i++) {
            StreamingLakeConfig.SchemaColumn c = cols.get(i);
            schemaCols.add(new StreamLakeSchema.Column(c.getName(), StreamLakeType.valueOf(c.getType())));
            if (c.isIndexed()) {
                indexed.add(i);
            }
        }
        return new StreamLakeTopicSchema(new StreamLakeSchema(schemaCols), indexed,
                config.getSetMaxCardinality(), config.getBloomFpp());
    }
}
