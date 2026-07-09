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
 * StreamLake client library: columnar (Apache Arrow) batch encode/decode, per-batch pruning stats
 * (min/max + exact-set/bloom) and their wire framing, the pushdown scan predicate, and the read-side
 * operators (per-column segment pruning, hash join, top-K). The producer batches rows and emits one
 * columnar message per batch; the consumer decodes it back to rows.
 */
package org.apache.pulsar.client.streaminglake;
