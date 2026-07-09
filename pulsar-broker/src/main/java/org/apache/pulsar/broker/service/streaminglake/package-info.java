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
/**
 * StreamLake: the broker-side metadata, segment-build and query tier of the columnar table store
 * layered on a Pulsar topic. The client encodes columnar (Apache Arrow) batches with a per-batch
 * stats footer; the broker persists them as normal entries and slices the footer into a durable
 * page-index ledger, a compaction pass merges page stats into segments, and the query tier prunes
 * (date &rarr; segment &rarr; page) and scans/joins. See {@code streamLake/DESIGN.md} at the repo
 * root for the end-to-end design and byte-level formats, and {@code streamLake/BENCHMARK.md} for the
 * inner-join benchmark and cost model.
 */
package org.apache.pulsar.broker.service.streaminglake;
