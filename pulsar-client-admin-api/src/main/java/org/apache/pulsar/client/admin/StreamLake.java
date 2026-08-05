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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.policies.data.StreamLakeQueryResult;
import org.apache.pulsar.common.policies.data.StreamLakeTableConfig;
import org.apache.pulsar.common.policies.data.StreamLakeTableInfo;

/**
 * Admin operations for StreamLake, the columnar analytical layer over a topic's data: register a topic
 * as a StreamLake table (schema + tuning), inspect its on-storage layout, and run SQL queries (a
 * single-table scan or a two-table inner equi-join over the StreamLake tables in a namespace, planned +
 * executed on the broker and returned as columns + rows).
 */
public interface StreamLake {

    /**
     * Register (or reconfigure) a topic as a StreamLake table by applying a {@link StreamLakeTableConfig}
     * (schema + tuning) as its topic policy. The topic must already exist and the broker must have
     * topic-level policies enabled. After this returns the topic accepts client-columnar writes and is
     * queryable by name.
     *
     * @param tenant    the tenant
     * @param namespace the namespace
     * @param table     the topic (table) short name within the namespace
     * @param config    the StreamLake table configuration to apply
     */
    void register(String tenant, String namespace, String table, StreamLakeTableConfig config)
            throws PulsarAdminException;

    /**
     * Report a StreamLake table's on-storage layout: data-ledger counts (total / segmented / open), the
     * backing catalog / page-index / segment ledgers, ingested rows and the event-time range.
     */
    StreamLakeTableInfo getInfo(String tenant, String namespace, String table) throws PulsarAdminException;

    /**
     * Run a StreamLake SQL query over the tables in {@code tenant/namespace} and return the result.
     * Supported: {@code SELECT &lt;cols|*&gt; FROM &lt;table&gt; [&lt;alias&gt;] [JOIN &lt;table&gt;
     * &lt;alias&gt; ON a.k=b.k] WHERE &lt;conjunctive predicates&gt;}. Table names refer to topics in
     * the given namespace.
     *
     * @param tenant    the tenant
     * @param namespace the namespace (its topics are the queryable tables)
     * @param sql       the query
     * @return the result columns + rows
     */
    StreamLakeQueryResult query(String tenant, String namespace, String sql) throws PulsarAdminException;

    /**
     * Run a StreamLake SQL query and receive the result as a <b>stream</b>: {@code handler.columns(...)}
     * once, then {@code handler.row(...)} per row as it arrives. Use this instead of
     * {@link #query(String, String, String)} for large results — rows are never all held in memory, so
     * a multi-GB result does not OOM the client.
     */
    void query(String tenant, String namespace, String sql, StreamLakeQueryResultHandler handler)
            throws PulsarAdminException;

    /** Asynchronously run a StreamLake SQL query. See {@link #query(String, String, String)}. */
    CompletableFuture<StreamLakeQueryResult> queryAsync(String tenant, String namespace, String sql);
}
