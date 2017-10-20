/**
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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Domain;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Sets;

@Parameters(commandDescription = "Operations about clusters")
public class CmdClusters extends CmdBase {

    @Parameters(commandDescription = "List the existing clusters")
    private class List extends CliCommand {
        void run() throws PulsarAdminException {
            print(admin.clusters().getClusters());
        }
    }

    @Parameters(commandDescription = "Get the configuration data for the specified cluster")
    private class Get extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getCluster(cluster));
        }
    }

    @Parameters(commandDescription = "Provisions a new cluster. This operation requires Pulsar super-user privileges")
    private class Create extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--url", description = "service-url", required = true)
        private String serviceUrl;

        @Parameter(names = "--url-secure", description = "service-url for secure connection", required = false)
        private String serviceUrlTls;
        
        @Parameter(names = "--broker-url", description = "broker-service-url", required = false)
        private String brokerServiceUrl;
        
        @Parameter(names = "--broker-url-secure", description = "broker-service-url for secure connection", required = false)
        private String brokerServiceUrlTls;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().createCluster(cluster,
                    new ClusterData(serviceUrl, serviceUrlTls, brokerServiceUrl, brokerServiceUrlTls));
        }
    }

    @Parameters(commandDescription = "Update the configuration for a cluster")
    private class Update extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--url", description = "service-url", required = true)
        private String serviceUrl;

        @Parameter(names = "--url-secure", description = "service-url for secure connection", required = false)
        private String serviceUrlTls;
        
        @Parameter(names = "--broker-url", description = "broker-service-url", required = false)
        private String brokerServiceUrl;
        
        @Parameter(names = "--broker-url-secure", description = "broker-service-url for secure connection", required = false)
        private String brokerServiceUrlTls;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().updateCluster(cluster,
                    new ClusterData(serviceUrl, serviceUrlTls, brokerServiceUrl, brokerServiceUrlTls));
        }
    }

    @Parameters(commandDescription = "Deletes an existing cluster")
    private class Delete extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().deleteCluster(cluster);
        }
    }

    @Parameters(commandDescription = "Create a new domain for a cluster. updates it if already created.")
    private class CreateDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Parameter(names = "--broker-list", description = "Comma separated broker list", required = false)
        private String brokerList;
        
        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            Domain domain = new Domain();
            domain.setBrokers((isNotBlank(brokerList) ? Sets.newHashSet(brokerList.split(",")): null));
            admin.clusters().createDomain(cluster, domainName, domain);
        }
    }

    @Parameters(commandDescription = "Update domain for a cluster. Creates a new one if not exist.")
    private class UpdateDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        @Parameter(names = "--broker-list", description = "Comma separated broker list", required = false)
        private String brokerList;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            Domain domain = new Domain();
            domain.setBrokers((isNotBlank(brokerList) ? Sets.newHashSet(brokerList.split(",")) : null));
            admin.clusters().updateDomain(cluster, domainName, domain);
        }
    }
    
    @Parameters(commandDescription = "Deletes an existing domain")
    private class DeleteDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            admin.clusters().deleteDomain(cluster, domainName);
        }
    }

    @Parameters(commandDescription = "List the existing domains for a cluster")
    private class ListDomains extends CliCommand {
        
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;
        
        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getDomains(cluster));
        }
    }

    @Parameters(commandDescription = "Get the configuration brokers of a domain")
    private class GetDomain extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;
        
        @Parameter(names = "--domain-name", description = "domain-name", required = true)
        private String domainName;

        void run() throws PulsarAdminException {
            String cluster = getOneArgument(params);
            print(admin.clusters().getDomain(cluster, domainName));
        }
    }
    
    public CmdClusters(PulsarAdmin admin) {
        super("clusters", admin);
        jcommander.addCommand("get", new Get());
        jcommander.addCommand("create", new Create());
        jcommander.addCommand("update", new Update());
        jcommander.addCommand("delete", new Delete());
        jcommander.addCommand("list", new List());
        jcommander.addCommand("get-domain", new GetDomain());
        jcommander.addCommand("create-domain", new CreateDomain());
        jcommander.addCommand("update-domain", new UpdateDomain());
        jcommander.addCommand("delete-domain", new DeleteDomain());
        jcommander.addCommand("list-domains", new ListDomains());
    }

}
