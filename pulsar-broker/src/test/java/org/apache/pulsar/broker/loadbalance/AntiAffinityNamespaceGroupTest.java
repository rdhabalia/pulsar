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
package org.apache.pulsar.broker.loadbalance;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.LocalBrokerData;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.TimeAverageMessageData;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerWrapper;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;

import junit.framework.Assert;

public class AntiAffinityNamespaceGroupTest {
    private LocalBookkeeperEnsemble bkEnsemble;

    private URL url1;
    private PulsarService pulsar1;
    private PulsarAdmin admin1;

    private URL url2;
    private PulsarService pulsar2;
    private PulsarAdmin admin2;

    private String primaryHost;
    private String secondaryHost;

    private NamespaceBundleFactory nsFactory;

    private ModularLoadManagerImpl primaryLoadManager;
    private ModularLoadManagerImpl secondaryLoadManager;

    private ExecutorService executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>());

    private final int ZOOKEEPER_PORT = PortManager.nextFreePort();
    private final int PRIMARY_BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();
    private final int SECONDARY_BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();
    private final int PRIMARY_BROKER_PORT = PortManager.nextFreePort();
    private final int SECONDARY_BROKER_PORT = PortManager.nextFreePort();
    private static final Logger log = LoggerFactory.getLogger(AntiAffinityNamespaceGroupTest.class);

    static {
        System.setProperty("test.basePort", "16100");
    }

    private static Object getField(final Object instance, final String fieldName) throws Exception {
        final Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

    @BeforeMethod
    void setup() throws Exception {

        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, ZOOKEEPER_PORT, PortManager.nextFreePort());
        bkEnsemble.start();

        // Start broker 1
        ServiceConfiguration config1 = new ServiceConfiguration();
        config1.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config1.setClusterName("use");
        config1.setWebServicePort(PRIMARY_BROKER_WEBSERVICE_PORT);
        config1.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config1.setBrokerServicePort(PRIMARY_BROKER_PORT);
        pulsar1 = new PulsarService(config1);

        pulsar1.start();

        primaryHost = String.format("%s:%d", InetAddress.getLocalHost().getHostName(), PRIMARY_BROKER_WEBSERVICE_PORT);
        url1 = new URL("http://127.0.0.1" + ":" + PRIMARY_BROKER_WEBSERVICE_PORT);
        admin1 = new PulsarAdmin(url1, (Authentication) null);

        // Start broker 2
        ServiceConfiguration config2 = new ServiceConfiguration();
        config2.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config2.setClusterName("use");
        config2.setWebServicePort(SECONDARY_BROKER_WEBSERVICE_PORT);
        config2.setZookeeperServers("127.0.0.1" + ":" + ZOOKEEPER_PORT);
        config2.setBrokerServicePort(SECONDARY_BROKER_PORT);
        pulsar2 = new PulsarService(config2);
        secondaryHost = String.format("%s:%d", InetAddress.getLocalHost().getHostName(),
                SECONDARY_BROKER_WEBSERVICE_PORT);

        pulsar2.start();

        url2 = new URL("http://127.0.0.1" + ":" + SECONDARY_BROKER_WEBSERVICE_PORT);
        admin2 = new PulsarAdmin(url2, (Authentication) null);

        primaryLoadManager = (ModularLoadManagerImpl) getField(pulsar1.getLoadManager().get(), "loadManager");
        secondaryLoadManager = (ModularLoadManagerImpl) getField(pulsar2.getLoadManager().get(), "loadManager");
        nsFactory = new NamespaceBundleFactory(pulsar1, Hashing.crc32());
        Thread.sleep(100);
    }

    @AfterMethod
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        executor.shutdown();

        admin1.close();
        admin2.close();

        pulsar2.close();
        pulsar1.close();

        bkEnsemble.stop();
    }

    
    @Test
    public void testClusterDomain() {
        
    }
    
    /**
     * 
     * It verifies anti-affinity-namespace assignment with failure-domain
     * 
     * <pre>
     * Domain     Brokers-count
     * ________  ____________
     * domain-0   broker-0,broker-1
     * domain-1   broker-2,broker-3
     * 
     * Anti-affinity-namespace assignment
     * 
     * (1) ns0 -> candidate-brokers: b0, b1, b2, b3 => selected b0
     * (2) ns1 -> candidate-brokers: b2, b3         => selected b2
     * (3) ns2 -> candidate-brokers: b1, b3         => selected b1
     * (4) ns3 -> candidate-brokers: b3             => selected b3
     * (5) ns4 -> candidate-brokers: b0, b1, b2, b3 => selected b0
     * 
     * "candidate" broker to own anti-affinity-namespace = b2 or b4
     * 
     * </pre>
     * 
     */
    @Test
    public void testAntiAffinityNamespaceFilteringWithDomain() throws Exception {

        final String namespace = "my-property/use/my-ns";
        final int totalNamespaces = 5;
        final String namespaceAntiAffinityGroup = "my-antiaffinity";
        final String bundle = "/0x00000000_0xffffffff";
        final int totalBrokers = 4;

        pulsar1.getConfiguration().setClusterDomainsEnabled(true);
        admin1.clusters().createCluster("use", new ClusterData("http://127.0.0.1:" + PRIMARY_BROKER_WEBSERVICE_PORT));
        admin1.properties().createProperty("my-property",
                new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));

        for (int i = 0; i < totalNamespaces; i++) {
            final String ns = namespace + i;
            admin1.namespaces().createNamespace(ns);
            admin1.namespaces().setNamespaceAntiAffinityGroup(ns, namespaceAntiAffinityGroup);
        }

        Set<String> brokers = Sets.newHashSet();
        Map<String, String> brokerToDomainMap = Maps.newHashMap();
        brokers.add("brokerName-0");
        brokerToDomainMap.put("brokerName-0", "domain-0");
        brokers.add("brokerName-1");
        brokerToDomainMap.put("brokerName-1", "domain-0");
        brokers.add("brokerName-2");
        brokerToDomainMap.put("brokerName-2", "domain-1");
        brokers.add("brokerName-3");
        brokerToDomainMap.put("brokerName-3", "domain-1");

        Set<String> candidate = Sets.newHashSet();
        Map<String, Map<String, Set<String>>> brokerToNamespaceToBundleRange = Maps.newHashMap();

        Assert.assertEquals(brokers.size(), totalBrokers);

        String assignedNamespace = namespace + "0" + bundle;
        candidate.addAll(brokers);

        // for namespace-0 all brokers available
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, brokers,
                brokerToNamespaceToBundleRange, brokerToDomainMap);
        Assert.assertEquals(brokers.size(), totalBrokers);

        // add namespace-0 to broker-0 of domain-0 => state: n0->b0
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "brokerName-0", namespace + "0", assignedNamespace);
        candidate.addAll(brokers);
        // for namespace-1 only domain-1 brokers are available as broker-0 already owns namespace-0
        assignedNamespace = namespace + "1" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, brokerToDomainMap);
        Assert.assertEquals(candidate.size(), 2);
        candidate.forEach(broker -> Assert.assertEquals(brokerToDomainMap.get(broker), "domain-1"));

        // add namespace-1 to broker-2 of domain-1 => state: n0->b0, n1->b2
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "brokerName-2", namespace + "1", assignedNamespace);
        candidate.addAll(brokers);
        // for namespace-2 only brokers available are : broker-1 and broker-3
        assignedNamespace = namespace + "2" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, brokerToDomainMap);
        Assert.assertEquals(candidate.size(), 2);
        Assert.assertTrue(candidate.contains("brokerName-1"));
        Assert.assertTrue(candidate.contains("brokerName-3"));

        // add namespace-2 to broker-1 of domain-0 => state: n0->b0, n1->b2, n2->b1
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "brokerName-1", namespace + "2", assignedNamespace);
        candidate.addAll(brokers);
        // for namespace-3 only brokers available are : broker-3
        assignedNamespace = namespace + "3" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, brokerToDomainMap);
        Assert.assertEquals(candidate.size(), 1);
        Assert.assertTrue(candidate.contains("brokerName-3"));
        // add namespace-3 to broker-3 of domain-1 => state: n0->b0, n1->b2, n2->b1, n3->b3
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "brokerName-3", namespace + "3", assignedNamespace);
        candidate.addAll(brokers);
        // for namespace-4 only brokers available are : all 4 brokers
        assignedNamespace = namespace + "4" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, brokerToDomainMap);
        Assert.assertEquals(candidate.size(), 4);
    }

    /**
     * It verifies anti-affinity-namespace assignment without failure-domain enabled
     * 
     * <pre>
     *  Anti-affinity-namespace assignment
     * 
     * (1) ns0 -> candidate-brokers: b0, b1, b2     => selected b0
     * (2) ns1 -> candidate-brokers: b1, b2         => selected b1
     * (3) ns2 -> candidate-brokers: b2             => selected b2
     * (5) ns3 -> candidate-brokers: b0, b1, b2     => selected b0
     * </pre>
     * 
     * 
     * @throws Exception
     */
    @Test
    public void testAntiAffinityNamespaceFilteringWithoutDomain() throws Exception {

        final String namespace = "my-property/use/my-ns";
        final int totalNamespaces = 5;
        final String namespaceAntiAffinityGroup = "my-antiaffinity";
        final String bundle = "/0x00000000_0xffffffff";

        admin1.clusters().createCluster("use", new ClusterData("http://127.0.0.1:" + PRIMARY_BROKER_WEBSERVICE_PORT));
        admin1.properties().createProperty("my-property",
                new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));

        for (int i = 0; i < totalNamespaces; i++) {
            final String ns = namespace + i;
            admin1.namespaces().createNamespace(ns);
            admin1.namespaces().setNamespaceAntiAffinityGroup(ns, namespaceAntiAffinityGroup);
        }

        Set<String> brokers = Sets.newHashSet();
        Set<String> candidate = Sets.newHashSet();
        Map<String, Map<String, Set<String>>> brokerToNamespaceToBundleRange = Maps.newHashMap();
        brokers.add("broker-0");
        brokers.add("broker-1");
        brokers.add("broker-2");

        String assignedNamespace = namespace + "0" + bundle;

        // all brokers available so, candidate will be all 3 brokers
        candidate.addAll(brokers);
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, brokers,
                brokerToNamespaceToBundleRange, null);
        Assert.assertEquals(brokers.size(), 3);

        // add ns-0 to broker-0
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "broker-0", namespace + "0", assignedNamespace);
        candidate.addAll(brokers);
        assignedNamespace = namespace + "1" + bundle;
        // available brokers for ns-1 => broker-1, broker-2
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, null);
        Assert.assertEquals(candidate.size(), 2);
        Assert.assertTrue(candidate.contains("broker-1"));
        Assert.assertTrue(candidate.contains("broker-2"));

        // add ns-1 to broker-1
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "broker-1", namespace + "1", assignedNamespace);
        candidate.addAll(brokers);
        // available brokers for ns-2 => broker-2
        assignedNamespace = namespace + "2" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, null);
        Assert.assertEquals(candidate.size(), 1);
        Assert.assertTrue(candidate.contains("broker-2"));

        // add ns-2 to broker-2
        selectBrokerForNamespace(brokerToNamespaceToBundleRange, "broker-2", namespace + "2", assignedNamespace);
        candidate.addAll(brokers);
        // available brokers for ns-3 => broker-0, broker-1, broker-2
        assignedNamespace = namespace + "3" + bundle;
        LoadManagerShared.filterAntiAffinityGroupOwnedBrokers(pulsar1, assignedNamespace, candidate,
                brokerToNamespaceToBundleRange, null);
        Assert.assertEquals(candidate.size(), 3);
    }

    private void selectBrokerForNamespace(Map<String, Map<String, Set<String>>> brokerToNamespaceToBundleRange,
            String broker, String namespace, String assignedBundleName) {
        Map<String, Set<String>> nsToBundleMap = Maps.newHashMap();
        nsToBundleMap.put(namespace, Sets.newHashSet(assignedBundleName));
        brokerToNamespaceToBundleRange.put(broker, nsToBundleMap);
    }

}
