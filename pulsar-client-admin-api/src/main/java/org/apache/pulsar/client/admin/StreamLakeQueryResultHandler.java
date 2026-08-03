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
package org.apache.pulsar.client.admin;

import java.util.List;

/**
 * Streaming callback for a StreamLake query: {@link #columns} is invoked once with the result header,
 * then {@link #row} is invoked once per result row as it arrives from the broker. Because rows are
 * consumed incrementally (never all held in memory), a multi-GB result can be processed without OOM on
 * the client.
 */
public interface StreamLakeQueryResultHandler {

    /** Called once, before any row, with the ordered result column names. */
    void columns(List<String> columns);

    /** Called once per result row, with the row's values in column order. */
    void row(List<Object> row);
}
