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
package org.apache.pulsar.broker.admin.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Utf8;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.streaminglake.StreamLakeCatalog;
import org.apache.pulsar.broker.service.streaminglake.StreamLakeMetaStore;
import org.apache.pulsar.broker.service.streaminglake.StreamLakeQueryCoordinator;
import org.apache.pulsar.broker.service.streaminglake.StreamLakeQueryService;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.StreamLakeQueryStats;
import org.apache.pulsar.common.policies.data.StreamLakeTableConfig;
import org.apache.pulsar.common.policies.data.StreamLakeTableInfo;
import org.apache.pulsar.common.policies.data.StreamingLakeConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Admin REST endpoint for submitting a StreamLake SQL query and <b>streaming</b> result rows. The query
 * is planned + executed by the broker-side {@link StreamLakeQueryCoordinator}: table names resolve to
 * topics <b>in the request's namespace</b>, and single-table scans and two-table inner joins are
 * supported. This is the transport behind {@code pulsar-admin streamlake query}.
 *
 * <p><b>Streaming (no broker OOM).</b> The query is planned first (so a bad query / unknown table
 * returns 400 before any bytes are sent); then the response is written as <b>NDJSON</b> — the first
 * line is a JSON array of column names, and each subsequent line is a JSON array of one row's values.
 * Rows are written straight to the response socket as they are produced and the writer is flushed
 * periodically, so a multi-GB result never materializes in broker heap.
 */
@Path("/streamlake")
@Tag(name = "streamlake")
@Produces({MediaType.APPLICATION_JSON, "application/x-ndjson"})
public class StreamLakeQuery extends AdminResource {

    private static final Logger log = LoggerFactory.getLogger(StreamLakeQuery.class);
    private static final ObjectMapper MAPPER = ObjectMapperFactory.getMapper().getObjectMapper();
    private static final int FLUSH_EVERY_ROWS = 1024;

    @POST
    @Path("/{tenant}/{namespace}/query")
    @Consumes(MediaType.TEXT_PLAIN)
    @Operation(summary = "Run a StreamLake SQL query (single-table scan or inner join) over the "
            + "namespace's tables and stream the result rows as NDJSON.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "NDJSON stream: line 1 = column names, "
                    + "each following line = one row (JSON arrays)."),
            @ApiResponse(responseCode = "400", description = "Malformed query or unknown/!StreamLake table.")
    })
    public void query(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            String sql) {
        validateNamespaceName(tenant, namespace);
        if (sql == null || sql.trim().isEmpty()) {
            asyncResponse.resume(new RestException(Response.Status.BAD_REQUEST, "Empty query"));
            return;
        }
        // Plan + resolve tables off the IO thread; a planning error becomes a clean 400 BEFORE we start
        // streaming (once the first byte is written the status is already 200).
        pulsar().getExecutor().execute(() -> {
            try {
                StreamLakeQueryCoordinator coordinator = new StreamLakeQueryCoordinator(
                        table -> resolveService(tenant, namespace, table));
                StreamLakeQueryCoordinator.Prepared prepared = coordinator.prepare(sql);
                StreamingOutput body = output -> writeNdjson(prepared, output);
                asyncResponse.resume(Response.ok(body).build());
            } catch (IllegalArgumentException e) {
                asyncResponse.resume(new RestException(Response.Status.BAD_REQUEST, e.getMessage()));
            } catch (Exception e) {
                log.warn("StreamLake query failed for {}/{}: {}", tenant, namespace, e.toString());
                asyncResponse.resume(new RestException(e));
            }
        });
    }

    @POST
    @Path("/{tenant}/{namespace}/{table}/register")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Register (or reconfigure) a topic as a StreamLake table by setting its "
            + "StreamingLakeConfig topic policy (schema, tuning, ledger rollovers, replication).")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "The table config was applied."),
            @ApiResponse(responseCode = "400", description = "Missing/invalid config."),
            @ApiResponse(responseCode = "404", description = "Topic does not exist.")
    })
    public void register(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("table") String table,
            StreamLakeTableConfig req) {
        validateNamespaceName(tenant, namespace);
        if (req == null || req.getColumns() == null || req.getColumns().isEmpty()) {
            asyncResponse.resume(new RestException(Response.Status.BAD_REQUEST,
                    "Missing StreamLakeTableConfig body (need at least one column)"));
            return;
        }
        StreamingLakeConfig config = toStreamingLakeConfig(req);
        String topicName = "persistent://" + tenant + "/" + namespace + "/" + table;
        pulsar().getTopicPoliciesService()
                .updateTopicPoliciesAsync(TopicName.get(topicName), false, false,
                        p -> p.setStreamingLake(config))
                .thenRun(() -> asyncResponse.resume(Response.noContent().build()))
                .exceptionally(ex -> {
                    log.warn("StreamLake register failed for {}: {}", topicName, ex.toString());
                    asyncResponse.resume(new RestException(ex));
                    return null;
                });
    }

    // Convert the lean admin-api request into the internal StreamingLakeConfig topic policy. The demo
    // uses a single replication factor (rf) for every StreamLake ledger tier (single-bookie).
    private static StreamingLakeConfig toStreamingLakeConfig(StreamLakeTableConfig req) {
        List<StreamingLakeConfig.SchemaColumn> cols = new ArrayList<>();
        for (StreamLakeTableConfig.Column c : req.getColumns()) {
            cols.add(new StreamingLakeConfig.SchemaColumn(c.getColumnId(), c.getName(), c.getType(),
                    c.isIndexed()));
        }
        int rf = Math.max(1, req.getReplicationFactor());
        return StreamingLakeConfig.builder()
                .enabled(true).clientColumnarEnabled(true)
                .setMaxCardinality(req.getSetMaxCardinality()).bloomFpp(req.getBloomFpp())
                .pageIndexEnsembleSize(rf).pageIndexWriteQuorum(rf).pageIndexAckQuorum(rf)
                .segmentEnsembleSize(rf).segmentWriteQuorum(rf).segmentAckQuorum(rf)
                .pageIndexMaxEntriesPerLedger(req.getPageIndexMaxEntriesPerLedger())
                .segmentMaxEntriesPerLedger(req.getSegmentMaxEntriesPerLedger())
                .asyncSegmentBuildViaSystemTopic(req.isAsyncSegmentBuildViaSystemTopic())
                .columns(cols).build();
    }

    @GET
    @Path("/{tenant}/{namespace}/{table}/info")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Report a StreamLake table's on-storage layout: data-ledger counts "
            + "(total/segmented/open), the catalog/page-index/segment ledgers, rows and event-time range.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The table's storage-layout snapshot."),
            @ApiResponse(responseCode = "400", description = "Not a loaded StreamLake table.")
    })
    public void info(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("tenant") String tenant,
            @PathParam("namespace") String namespace,
            @PathParam("table") String table) {
        validateNamespaceName(tenant, namespace);
        pulsar().getExecutor().execute(() -> {
            try {
                asyncResponse.resume(buildInfo(tenant, namespace, table));
            } catch (IllegalArgumentException e) {
                asyncResponse.resume(new RestException(Response.Status.BAD_REQUEST, e.getMessage()));
            } catch (Exception e) {
                log.warn("StreamLake info failed for {}/{}/{}: {}", tenant, namespace, table, e.toString());
                asyncResponse.resume(new RestException(e));
            }
        });
    }

    private StreamLakeTableInfo buildInfo(String tenant, String namespace, String table) throws Exception {
        String topicName = "persistent://" + tenant + "/" + namespace + "/" + table;
        Optional<Topic> ref = pulsar().getBrokerService().getTopicReference(topicName);
        if (ref.isEmpty()) {
            ref = pulsar().getBrokerService().getTopic(topicName, false).get();
        }
        if (ref.isEmpty() || !(ref.get() instanceof PersistentTopic)) {
            throw new IllegalArgumentException("Not a loaded StreamLake table: " + table);
        }
        PersistentTopic pt = (PersistentTopic) ref.get();
        Map<Long, StreamLakeCatalog.LedgerInfo> catalog = pt.getStreamLakeSegmentService().catalog().all();
        StreamLakeMetaStore.Record rec = new StreamLakeMetaStore(
                pulsar().getLocalMetadataStore(), pt.getManagedLedger()).read();

        StreamLakeTableInfo info = new StreamLakeTableInfo();
        info.setTopic(topicName);
        info.setDataLedgers(catalog.size());
        long segmented = 0;
        long pages = 0;
        long minEt = Long.MAX_VALUE;
        long maxEt = Long.MIN_VALUE;
        for (StreamLakeCatalog.LedgerInfo li : catalog.values()) {
            if (li.state == StreamLakeCatalog.State.SEGMENTED) {
                segmented++;
            }
            pages += li.rowCount; // LedgerInfo.rowCount is the data ledger's entry (page) count
            minEt = Math.min(minEt, li.minEventTime);
            maxEt = Math.max(maxEt, li.maxEventTime);
        }
        info.setDataLedgersSegmented(segmented);
        info.setDataLedgersOpen(catalog.size() - segmented);
        info.setDataPages(pages);
        info.setMinEventTime(catalog.isEmpty() ? 0 : minEt);
        info.setMaxEventTime(catalog.isEmpty() ? 0 : maxEt);
        info.setCatalogLedgerId(rec.catalogLedgerId);
        info.setPageIndexLedgerIds(rec.pageIndexLedgerIds);
        info.setSegmentLedgerIds(rec.segmentLedgerIds);
        return info;
    }

    private static void writeNdjson(StreamLakeQueryCoordinator.Prepared prepared, OutputStream os)
            throws IOException {
        long t0 = System.nanoTime();
        BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
        w.write(MAPPER.writeValueAsString(prepared.columns()));
        w.write('\n');
        long[] rows = {0};
        long[] bytes = {0};
        try {
            prepared.stream(row -> {
                String line = MAPPER.writeValueAsString(row);
                w.write(line);
                w.write('\n');
                rows[0]++;
                bytes[0] += Utf8.encodedLength(line) + 1;
                if ((rows[0] % FLUSH_EVERY_ROWS) == 0) {
                    w.flush();
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("StreamLake query streaming failed: " + e.getMessage(), e);
        }
        // Trailing NDJSON metadata: a JSON OBJECT (rows are arrays, so it is unambiguous) carrying the
        // execution stats -- the client parses it out and prints it as a footer.
        StreamLakeQueryStats stats = prepared.metrics().toStats();
        stats.setRowsReturned(rows[0]);
        stats.setBytesReturned(bytes[0]);
        stats.setElapsedMs((System.nanoTime() - t0) / 1_000_000);
        w.write(MAPPER.writeValueAsString(stats));
        w.write('\n');
        w.flush();
    }

    // Resolve a table name to its per-topic query service (loading the topic on this broker if needed).
    private StreamLakeQueryService resolveService(String tenant, String namespace, String table) {
        String topicName = "persistent://" + tenant + "/" + namespace + "/" + table;
        try {
            Optional<Topic> ref = pulsar().getBrokerService().getTopicReference(topicName);
            if (ref.isEmpty()) {
                ref = pulsar().getBrokerService().getTopic(topicName, false).get();
            }
            if (ref.isEmpty() || !(ref.get() instanceof PersistentTopic)) {
                return null;
            }
            return ((PersistentTopic) ref.get()).getStreamLakeQueryService();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to resolve table " + table + ": " + e.getMessage(), e);
        }
    }
}
