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
package org.apache.pulsar.admin.cli;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.StreamLakeQueryResultHandler;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * {@code pulsar-admin streamlake} — StreamLake columnar query operations. The {@code query} subcommand
 * submits a SQL query (single-table scan or two-table inner equi-join) over the StreamLake tables in a
 * namespace to the broker and prints the result rows as they <b>stream</b> back (the CLI holds only one
 * row at a time, so a large result does not OOM the client).
 */
@Command(description = "Operations about StreamLake (columnar analytical queries over topics)")
public class CmdStreamLake extends CmdBase {

    public CmdStreamLake(Supplier<PulsarAdmin> admin) {
        super("streamlake", admin);
        addCommand("query", new Query());
    }

    @Command(description = "Run a StreamLake SQL query over a namespace's tables and print the result. "
            + "Supports SELECT <cols|*> FROM <table> [alias] [JOIN <table> alias ON a.k=b.k] WHERE "
            + "<conjunctive predicates>. Table names are topics in the namespace. Streams the result.")
    private class Query extends CliCommand {
        @Parameters(index = "0", description = "tenant/namespace whose topics are the tables", arity = "1")
        private String namespace;

        @Parameters(index = "1", description = "the SQL query", arity = "1")
        private String sql;

        @Option(names = {"-j", "--json"}, description = "Print each row as a JSON array (NDJSON) "
                + "instead of a table")
        private boolean json;

        @Override
        void run() throws Exception {
            NamespaceName ns = NamespaceName.get(validateNamespace(namespace));
            final long[] count = {0};
            getAdmin().streamLake().query(ns.getTenant(), ns.getLocalName(), sql,
                    new StreamLakeQueryResultHandler() {
                        @Override
                        public void columns(List<String> columns) {
                            print(json ? toJson(columns) : String.join(" | ", columns));
                        }

                        @Override
                        public void row(List<Object> row) {
                            count[0]++;
                            print(json ? toJson(row) : row.stream()
                                    .map(v -> v == null ? "null" : v.toString())
                                    .collect(Collectors.joining(" | ")));
                        }
                    });
            if (!json) {
                print(String.format("(%,d rows)", count[0]));
            }
        }

        private String toJson(Object value) {
            try {
                return ObjectMapperFactory.getMapper().getObjectMapper().writeValueAsString(value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
