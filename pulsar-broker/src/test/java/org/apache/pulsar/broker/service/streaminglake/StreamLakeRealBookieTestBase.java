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
package org.apache.pulsar.broker.service.streaminglake;

import java.util.Collections;
import java.util.Optional;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * Test base that runs a real broker against a real {@link LocalBookkeeperEnsemble}
 * ({@code DbLedgerStorage} with the StreamLake page index). Unlike the mocked-BookKeeper
 * harness, this lets StreamLake code exercise the real {@code addEntry}-with-ranges path and
 * bookie {@code PAGE_PRUNE}. The broker's own BookKeeper client is available via
 * {@code pulsar.getBookKeeperClientFactory()}/{@code pulsar.getBookKeeperClient()}.
 */
public abstract class StreamLakeRealBookieTestBase {

    protected static final String CLUSTER = "streamlake-cluster";
    protected static final String TENANT = "streamlake";
    protected static final String NAMESPACE = TENANT + "/ns";

    protected LocalBookkeeperEnsemble bkEnsemble;
    protected ServiceConfiguration config;
    protected PulsarService pulsar;
    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;

    @BeforeMethod(alwaysRun = true)
    void setup() throws Exception {
        bkEnsemble = new LocalBookkeeperEnsemble(1, 0);
        bkEnsemble.start();

        config = new ServiceConfiguration();
        config.setClusterName(CLUSTER);
        config.setWebServicePort(Optional.of(0));
        config.setBrokerServicePort(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setAdvertisedAddress("localhost");
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setLoadManagerClassName(SimpleLoadManagerImpl.class.getName());
        config.setLoadBalancerEnabled(false);
        config.setSystemTopicEnabled(true);
        config.setTopicLevelPoliciesEnabled(true);

        pulsar = new PulsarService(config);
        pulsar.start();

        String httpUrl = pulsar.getWebServiceAddress();
        admin = PulsarAdmin.builder().serviceHttpUrl(httpUrl).build();
        admin.clusters().createCluster(CLUSTER, ClusterData.builder().serviceUrl(httpUrl).build());
        admin.tenants().createTenant(TENANT, TenantInfo.builder()
                .allowedClusters(Collections.singleton(CLUSTER)).build());
        admin.namespaces().createNamespace(NAMESPACE, Collections.singleton(CLUSTER));

        pulsarClient = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
    }

    @AfterMethod(alwaysRun = true)
    void cleanup() throws Exception {
        if (pulsarClient != null) {
            pulsarClient.close();
            pulsarClient = null;
        }
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (pulsar != null) {
            pulsar.close();
            pulsar = null;
        }
        if (bkEnsemble != null) {
            bkEnsemble.stop();
            bkEnsemble = null;
        }
    }
}
