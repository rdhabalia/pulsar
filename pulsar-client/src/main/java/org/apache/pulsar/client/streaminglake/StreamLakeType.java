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

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** The StreamLake logical column types and their fixed mapping to Apache Arrow types. */
public enum StreamLakeType {
    INT32(new ArrowType.Int(32, true)),
    INT64(new ArrowType.Int(64, true)),
    DOUBLE(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
    BOOLEAN(new ArrowType.Bool()),
    STRING(new ArrowType.Utf8()),
    BYTES(new ArrowType.Binary());

    private final ArrowType arrowType;

    StreamLakeType(ArrowType arrowType) {
        this.arrowType = arrowType;
    }

    /** The Apache Arrow type this logical type encodes to. */
    public ArrowType arrowType() {
        return arrowType;
    }
}
