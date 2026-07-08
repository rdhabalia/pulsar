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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * An ordered StreamLake table schema: the column names/types a producer encodes into an Arrow batch.
 * Immutable; converts to an Apache Arrow {@link Schema} for encode/decode.
 */
public final class StreamLakeSchema {

    /** A single named, typed column. */
    public static final class Column {
        private final String name;
        private final StreamLakeType type;

        public Column(String name, StreamLakeType type) {
            this.name = name;
            this.type = type;
        }

        public String name() {
            return name;
        }

        public StreamLakeType type() {
            return type;
        }
    }

    private final List<Column> columns;

    public StreamLakeSchema(List<Column> columns) {
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    }

    public List<Column> columns() {
        return columns;
    }

    public int size() {
        return columns.size();
    }

    /** The equivalent Apache Arrow schema (all columns nullable). */
    public Schema toArrowSchema() {
        List<Field> fields = new ArrayList<>(columns.size());
        for (Column c : columns) {
            fields.add(new Field(c.name(), FieldType.nullable(c.type().arrowType()), null));
        }
        return new Schema(fields);
    }
}
