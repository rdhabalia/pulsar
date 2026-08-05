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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.StreamLakeQueryResultHandler;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.StreamLakeQueryStats;
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
            if (json) {
                runJson(ns);
            } else {
                runTable(ns);
            }
        }

        // --json: stream each row (and the trailing stats) as JSON, one per line (no client buffering).
        private void runJson(NamespaceName ns) throws Exception {
            getAdmin().streamLake().query(ns.getTenant(), ns.getLocalName(), sql,
                    new StreamLakeQueryResultHandler() {
                        @Override
                        public void columns(List<String> columns) {
                            print(toJson(columns));
                        }

                        @Override
                        public void row(List<Object> row) {
                            print(toJson(row));
                        }

                        @Override
                        public void summary(StreamLakeQueryStats stats) {
                            print(toJson(stats));
                        }
                    });
        }

        // Default: buffer the rows (needed to size columns), print an aligned table, then a stats footer.
        private void runTable(NamespaceName ns) throws Exception {
            List<String> cols = new ArrayList<>();
            List<List<Object>> rows = new ArrayList<>();
            StreamLakeQueryStats[] stats = {null};
            getAdmin().streamLake().query(ns.getTenant(), ns.getLocalName(), sql,
                    new StreamLakeQueryResultHandler() {
                        @Override
                        public void columns(List<String> columns) {
                            cols.addAll(columns);
                        }

                        @Override
                        public void row(List<Object> row) {
                            rows.add(row);
                        }

                        @Override
                        public void summary(StreamLakeQueryStats s) {
                            stats[0] = s;
                        }
                    });
            printTable(cols, rows);
            print(String.format("(%,d rows)", rows.size()));
            if (stats[0] != null) {
                printFooter(stats[0]);
            }
        }

        private void printTable(List<String> cols, List<List<Object>> rows) {
            int n = cols.size();
            int[] width = new int[n];
            for (int i = 0; i < n; i++) {
                width[i] = cols.get(i) == null ? 4 : cols.get(i).length();
            }
            List<String[]> cells = new ArrayList<>(rows.size());
            for (List<Object> row : rows) {
                String[] c = new String[n];
                for (int i = 0; i < n; i++) {
                    Object v = i < row.size() ? row.get(i) : null;
                    c[i] = v == null ? "null" : v.toString();
                    width[i] = Math.max(width[i], c[i].length());
                }
                cells.add(c);
            }
            print(renderRow(cols.toArray(new String[0]), width));
            print(renderSeparator(width));
            for (String[] c : cells) {
                print(renderRow(c, width));
            }
        }

        private void printFooter(StreamLakeQueryStats s) {
            print("");
            print(String.format("rows returned: %,d   rows read: %,d",
                    s.getRowsReturned(), s.getRowsRead()));
            print(String.format("pages scanned: %,d   kept: %,d   pruned: %,d   candidate ledgers: %,d",
                    s.getPagesScanned(), s.getPagesKept(), s.getPagesPruned(), s.getCandidateLedgers()));
            print(String.format("bytes read: %s   bytes returned: %s   peak read buffer: %s",
                    humanBytes(s.getBytesRead()), humanBytes(s.getBytesReturned()),
                    humanBytes(s.getPeakReadBufferBytes())));
            print(String.format("elapsed: %,d ms", s.getElapsedMs()));
        }

        private String toJson(Object value) {
            try {
                return ObjectMapperFactory.getMapper().getObjectMapper().writeValueAsString(value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static String renderRow(String[] cells, int[] width) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < width.length; i++) {
            if (i > 0) {
                sb.append(" | ");
            }
            String v = i < cells.length && cells[i] != null ? cells[i] : "";
            sb.append(v);
            for (int p = v.length(); p < width[i]; p++) {
                sb.append(' ');
            }
        }
        return sb.toString();
    }

    private static String renderSeparator(int[] width) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < width.length; i++) {
            if (i > 0) {
                sb.append("-+-");
            }
            for (int p = 0; p < width[i]; p++) {
                sb.append('-');
            }
        }
        return sb.toString();
    }

    private static String humanBytes(long b) {
        if (b < 1024) {
            return b + " B";
        }
        String[] u = {"KiB", "MiB", "GiB", "TiB", "PiB"};
        double v = b;
        int i = -1;
        do {
            v /= 1024;
            i++;
        } while (v >= 1024 && i < u.length - 1);
        return String.format("%.1f %s", v, u[i]);
    }
}
