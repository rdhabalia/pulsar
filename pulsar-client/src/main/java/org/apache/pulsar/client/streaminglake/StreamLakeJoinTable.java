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

import java.util.List;

/**
 * The build-side table of a StreamLake hash join: a multimap from join key to the build rows carrying
 * that key. Pluggable so the join can trade memory for scale — {@link OnHeapJoinTable} keeps rows in
 * the JVM heap (fast, default), while {@link SpillingJoinTable} keeps only the key index on-heap and
 * spills the row bytes to a file (for build sides larger than RAM). The probe side is identical for
 * either backend: look up the key, emit for each match.
 */
public interface StreamLakeJoinTable extends AutoCloseable {

    /** Add a build row under its (non-null) join key. */
    void add(Object key, Object[] row);

    /** The build rows carrying {@code key} (empty list if none). */
    List<Object[]> get(Object key);

    /** Number of build rows added. */
    long size();

    /** Release any resources (e.g. a spill file). */
    @Override
    void close();
}
