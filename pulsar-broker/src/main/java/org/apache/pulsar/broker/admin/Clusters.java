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
package org.apache.pulsar.broker.admin;

import static org.apache.pulsar.broker.cache.ConfigurationCacheService.POLICIES;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Domain;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Maps;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/clusters")
@Api(value = "/clusters", description = "Cluster admin apis", tags = "clusters")
@Produces(MediaType.APPLICATION_JSON)
public class Clusters extends AdminResource {

    @GET
    @ApiOperation(value = "Get the list of all the Pulsar clusters.", response = String.class, responseContainer = "Set")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public Set<String> getClusters() throws Exception {
        try {
            return clustersListCache().get();
        } catch (Exception e) {
            log.error("[{}] Failed to get clusters list", clientAppId(), e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{cluster}")
    @ApiOperation(value = "Get the configuration data for the specified cluster.", response = ClusterData.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Cluster doesn't exist") })
    public ClusterData getCluster(@PathParam("cluster") String cluster) {
        validateSuperUserAccess();

        try {
            return clustersCache().get(path("clusters", cluster))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Cluster does not exist"));
        } catch (Exception e) {
            log.error("[{}] Failed to get cluster {}", clientAppId(), cluster, e);
            if (e instanceof RestException) {
                throw (RestException) e;
            } else {
                throw new RestException(e);
            }
        }
    }

    @PUT
    @Path("/{cluster}")
    @ApiOperation(value = "Provisions a new cluster. This operation requires Pulsar super-user privileges.", notes = "The name cannot contain '/' characters.")
    @ApiResponses(value = { @ApiResponse(code = 204, message = "Cluster has been created"),
            @ApiResponse(code = 403, message = "You don't have admin permission to create the cluster"),
            @ApiResponse(code = 409, message = "Cluster already exists"),
            @ApiResponse(code = 412, message = "Cluster name is not valid") })
    public void createCluster(@PathParam("cluster") String cluster, ClusterData clusterData) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            NamedEntity.checkName(cluster);
            zkCreate(path("clusters", cluster), jsonMapper().writeValueAsBytes(clusterData));
            log.info("[{}] Created cluster {}", clientAppId(), cluster);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create already existing cluster {}", clientAppId(), cluster);
            throw new RestException(Status.CONFLICT, "Cluster already exist");
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Failed to create cluster with invalid name {}", clientAppId(), cluster, e);
            throw new RestException(Status.PRECONDITION_FAILED, "Cluster name is not valid");
        } catch (Exception e) {
            log.error("[{}] Failed to create cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/{cluster}")
    @ApiOperation(value = "Update the configuration for a cluster.", notes = "This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 204, message = "Cluster has been updated"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Cluster doesn't exist") })
    public void updateCluster(@PathParam("cluster") String cluster, ClusterData clusterData) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        try {
            String clusterPath = path("clusters", cluster);
            globalZk().setData(clusterPath, jsonMapper().writeValueAsBytes(clusterData), -1);
            globalZkCache().invalidate(clusterPath);
            log.info("[{}] Updated cluster {}", clientAppId(), cluster);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to update cluster {}: Does not exist", clientAppId(), cluster);
            throw new RestException(Status.NOT_FOUND, "Cluster does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

    @DELETE
    @Path("/{cluster}")
    @ApiOperation(value = "Delete an existing cluster")
    @ApiResponses(value = { @ApiResponse(code = 204, message = "Cluster has been updated"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Cluster doesn't exist"),
            @ApiResponse(code = 412, message = "Cluster is not empty") })
    public void deleteCluster(@PathParam("cluster") String cluster) {
        validateSuperUserAccess();
        validatePoliciesReadOnlyAccess();

        // Check that the cluster is not used by any property (eg: no namespaces provisioned there)
        boolean isClusterUsed = false;
        try {
            for (String property : globalZk().getChildren(path(POLICIES), false)) {
                if (globalZk().exists(path(POLICIES, property, cluster), false) == null) {
                    continue;
                }

                if (!globalZk().getChildren(path(POLICIES, property, cluster), false).isEmpty()) {
                    // We found a property that has at least a namespace in this cluster
                    isClusterUsed = true;
                    break;
                }
            }

            // check the namespaceIsolationPolicies associated with the cluster
            String path = path("clusters", cluster, "namespaceIsolationPolicies");
            Optional<NamespaceIsolationPolicies> nsIsolationPolicies = namespaceIsolationPoliciesCache().get(path);

            // Need to delete the isolation policies if present
            if (nsIsolationPolicies.isPresent()) {
                if (nsIsolationPolicies.get().getPolicies().isEmpty()) {
                    globalZk().delete(path, -1);
                    namespaceIsolationPoliciesCache().invalidate(path);
                } else {
                    isClusterUsed = true;
                }
            }
        } catch (Exception e) {
            log.error("[{}] Failed to get cluster usage {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }

        if (isClusterUsed) {
            log.warn("[{}] Failed to delete cluster {} - Cluster not empty", clientAppId(), cluster);
            throw new RestException(Status.PRECONDITION_FAILED, "Cluster not empty");
        }

        try {
            String clusterPath = path("clusters", cluster);
            globalZk().delete(clusterPath, -1);
            globalZkCache().invalidate(clusterPath);
            log.info("[{}] Deleted cluster {}", clientAppId(), cluster);
        } catch (KeeperException.NoNodeException e) {
            log.warn("[{}] Failed to delete cluster {} - Does not exist", clientAppId(), cluster);
            throw new RestException(Status.NOT_FOUND, "Cluster does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to delete cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{cluster}/namespaceIsolationPolicies")
    @ApiOperation(value = "Get the namespace isolation policies assigned in the cluster", response = NamespaceIsolationData.class, responseContainer = "Map")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Cluster doesn't exist") })
    public Map<String, NamespaceIsolationData> getNamespaceIsolationPolicies(@PathParam("cluster") String cluster)
            throws Exception {
        validateSuperUserAccess();
        if (!clustersCache().get(path("clusters", cluster)).isPresent()) {
            throw new RestException(Status.NOT_FOUND, "Cluster " + cluster + " does not exist.");
        }

        try {
            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPoliciesCache()
                    .get(path("clusters", cluster, "namespaceIsolationPolicies"))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                            "NamespaceIsolationPolicies for cluster " + cluster + " does not exist"));
            // construct the response to NamespaceisolationData map
            return nsIsolationPolicies.getPolicies();
        } catch (Exception e) {
            log.error("[{}] Failed to get clusters/{}/namespaceIsolationPolicies", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

    private void validateClusterExists(String cluster) {
        try {
            if (!clustersCache().get(path("clusters", cluster)).isPresent()) {
                throw new RestException(Status.PRECONDITION_FAILED, "Cluster " + cluster + " does not exist.");
            }
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{cluster}/namespaceIsolationPolicies/{policyName}")
    @ApiOperation(value = "Get a single namespace isolation policy assigned in the cluster", response = NamespaceIsolationData.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Policy doesn't exist"),
            @ApiResponse(code = 412, message = "Cluster doesn't exist") })
    public NamespaceIsolationData getNamespaceIsolationPolicy(@PathParam("cluster") String cluster,
            @PathParam("policyName") String policyName) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);

        try {
            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPoliciesCache()
                    .get(path("clusters", cluster, "namespaceIsolationPolicies"))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                            "NamespaceIsolationPolicies for cluster " + cluster + " does not exist"));
            // construct the response to NamespaceisolationData map
            if (!nsIsolationPolicies.getPolicies().containsKey(policyName)) {
                log.info("[{}] Cannot find NamespaceIsolationPolicy {} for cluster {}", policyName, cluster);
                throw new RestException(Status.NOT_FOUND,
                        "Cannot find NamespaceIsolationPolicy " + policyName + " for cluster " + cluster);
            }
            return nsIsolationPolicies.getPolicies().get(policyName);
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get clusters/{}/namespaceIsolationPolicies/{}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/{cluster}/namespaceIsolationPolicies/{policyName}")
    @ApiOperation(value = "Set namespace isolation policy")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission or plicy is read only"),
            @ApiResponse(code = 412, message = "Cluster doesn't exist") })
    public void setNamespaceIsolationPolicy(@PathParam("cluster") String cluster,
            @PathParam("policyName") String policyName, NamespaceIsolationData policyData) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);
        validatePoliciesReadOnlyAccess();

        try {
            // validate the policy data before creating the node
            policyData.validate();

            String nsIsolationPolicyPath = path("clusters", cluster, "namespaceIsolationPolicies");
            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPoliciesCache()
                    .get(nsIsolationPolicyPath).orElseGet(() -> {
                        try {
                            this.createZnodeIfNotExist(nsIsolationPolicyPath, Collections.emptyMap());
                            return new NamespaceIsolationPolicies();
                        } catch (KeeperException | InterruptedException e) {
                            throw new RestException(e);
                        }
                    });

            nsIsolationPolicies.setPolicy(policyName, policyData);
            globalZk().setData(nsIsolationPolicyPath, jsonMapper().writeValueAsBytes(nsIsolationPolicies.getPolicies()),
                    -1);
            // make sure that the cache content will be refreshed for the next read access
            namespaceIsolationPoliciesCache().invalidate(nsIsolationPolicyPath);
        } catch (IllegalArgumentException iae) {
            log.info("[{}] Failed to update clusters/{}/namespaceIsolationPolicies/{}. Input data is invalid",
                    clientAppId(), cluster, policyName, iae);
            String jsonInput = ObjectMapperFactory.create().writeValueAsString(policyData);
            throw new RestException(Status.BAD_REQUEST,
                    "Invalid format of input policy data. policy: " + policyName + "; data: " + jsonInput);
        } catch (KeeperException.NoNodeException nne) {
            log.warn("[{}] Failed to update clusters/{}/namespaceIsolationPolicies: Does not exist", clientAppId(),
                    cluster);
            throw new RestException(Status.NOT_FOUND,
                    "NamespaceIsolationPolicies for cluster " + cluster + " does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update clusters/{}/namespaceIsolationPolicies/{}", clientAppId(), cluster,
                    policyName, e);
            throw new RestException(e);
        }
    }

    private boolean createZnodeIfNotExist(String path, Object value) throws KeeperException, InterruptedException {
        // create persistent node on ZooKeeper
        if (globalZk().exists(path, false) == null) {
            // create all the intermediate nodes
            try {
                ZkUtils.createFullPathOptimistic(globalZk(), path,
                        value != null ? jsonMapper().writeValueAsBytes(value) : null, Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                return true;
            } catch (KeeperException.NodeExistsException nee) {
                log.debug("Other broker preempted the full path [{}] already. Continue...", path);
            } catch (JsonGenerationException e) {
                // ignore json error as it is empty hash
            } catch (JsonMappingException e) {
            } catch (IOException e) {
            }
        }
        return false;
    }

    @DELETE
    @Path("/{cluster}/namespaceIsolationPolicies/{policyName}")
    @ApiOperation(value = "Delete namespace isolation policy")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission or plicy is read only"),
            @ApiResponse(code = 412, message = "Cluster doesn't exist") })
    public void deleteNamespaceIsolationPolicy(@PathParam("cluster") String cluster,
            @PathParam("policyName") String policyName) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);
        validatePoliciesReadOnlyAccess();

        try {

            String nsIsolationPolicyPath = path("clusters", cluster, "namespaceIsolationPolicies");
            NamespaceIsolationPolicies nsIsolationPolicies = namespaceIsolationPoliciesCache()
                    .get(nsIsolationPolicyPath).orElseGet(() -> {
                        try {
                            this.createZnodeIfNotExist(nsIsolationPolicyPath, Collections.emptyMap());
                            return new NamespaceIsolationPolicies();
                        } catch (KeeperException | InterruptedException e) {
                            throw new RestException(e);
                        }
                    });

            nsIsolationPolicies.deletePolicy(policyName);
            globalZk().setData(nsIsolationPolicyPath, jsonMapper().writeValueAsBytes(nsIsolationPolicies.getPolicies()),
                    -1);
            // make sure that the cache content will be refreshed for the next read access
            namespaceIsolationPoliciesCache().invalidate(nsIsolationPolicyPath);
        } catch (KeeperException.NoNodeException nne) {
            log.warn("[{}] Failed to update brokers/{}/namespaceIsolationPolicies: Does not exist", clientAppId(),
                    cluster);
            throw new RestException(Status.NOT_FOUND,
                    "NamespaceIsolationPolicies for cluster " + cluster + " does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update brokers/{}/namespaceIsolationPolicies/{}", clientAppId(), cluster,
                    policyName, e);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/{cluster}/domains/{domainName}")
    @ApiOperation(value = "Set cluster's domain")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Broker already exist into other domain"),
            @ApiResponse(code = 412, message = "Cluster doesn't exist") })
    public void setDomain(@PathParam("cluster") String cluster, @PathParam("domainName") String domainName,
            Domain domain) throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);
        validateBrokerExistsInOtherDomain(cluster, domainName, domain);

        try {
            this.createZnodeIfNotExist(path("clusters", cluster, "domains"), null);
            String domainPath = path("clusters", cluster, "domains", domainName);
            if (this.createZnodeIfNotExist(domainPath, domain)) {
                // clear children cache and reload for further watch
                //this.clusterDomainListCache().clear();
                //this.clusterDomainListCache().get();
            } else {
                globalZk().setData(domainPath, jsonMapper().writeValueAsBytes(domain), -1);
            }
            // make sure that the cache content will be refreshed for the next read access
            domainCache().invalidate(domainPath);
        } catch (IllegalArgumentException iae) {
            log.info("[{}] Failed to update clusters/{}/domainName/{}. Input data is invalid", clientAppId(), cluster,
                    domainName, iae);
            String jsonInput = ObjectMapperFactory.create().writeValueAsString(domainName);
            throw new RestException(Status.BAD_REQUEST,
                    "Invalid format of input domain data. domainName: " + domainName + "; data: " + jsonInput);
        } catch (KeeperException.NoNodeException nne) {
            log.warn("[{}] Failed to update clusters/{}/domainName: Does not exist", clientAppId(), cluster);
            throw new RestException(Status.NOT_FOUND,
                    "Domain "+ domainName +" for cluster " + cluster + " does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to update clusters/{}/domainName/{}", clientAppId(), cluster, domainName, e);
            throw new RestException(e);
        }
    }

    @GET
    @Path("/{cluster}/domains")
    @ApiOperation(value = "Get the cluster domains", response = Domain.class, responseContainer = "Map")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Cluster doesn't exist") })
    public Map<String, Domain> getDomains(@PathParam("cluster") String cluster) throws Exception {
        validateSuperUserAccess();

        Map<String, Domain> domains = Maps.newHashMap();
        try {
            for (String domainName : clusterDomainListCache().get()) {
                try {
                    Optional<Domain> domain = domainCache().get(path("clusters", cluster, "domains", domainName));
                    if (domain.isPresent()) {
                        domains.put(domainName, domain.get());
                    }
                } catch (Exception e) {
                    log.warn("Failed to get domain {}", domainName, e);
                }
            }
        } catch (KeeperException.NoNodeException e) {
            log.error("[{}] Domain is not configured for cluster {}", clientAppId(), cluster, e);
            return Collections.emptyMap();
        } catch (Exception e) {
            log.error("[{}] Failed to get domains for cluster {}", clientAppId(), cluster, e);
            throw new RestException(e);
        }
        return domains;
    }

    @GET
    @Path("/{cluster}/domains/{domainName}")
    @ApiOperation(value = "Get a domain in a cluster", response = Domain.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Domain doesn't exist"),
            @ApiResponse(code = 412, message = "Cluster doesn't exist") })
    public Domain getDomain(@PathParam("cluster") String cluster, @PathParam("domainName") String domainName)
            throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);

        try {
            return domainCache().get(path("clusters", cluster, "domains", domainName))
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                            "Domain " + domainName + " for cluster " + cluster + " does not exist"));
        } catch (RestException re) {
            throw re;
        } catch (Exception e) {
            log.error("[{}] Failed to get domain {} for cluster {}", clientAppId(), domainName, cluster, e);
            throw new RestException(e);
        }
    }

    @DELETE
    @Path("/{cluster}/domains/{domainName}")
    @ApiOperation(value = "Delete cluster's domain")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission or plicy is read only"),
            @ApiResponse(code = 412, message = "Cluster doesn't exist") })
    public void deleteDomain(@PathParam("cluster") String cluster, @PathParam("domainName") String domainName)
            throws Exception {
        validateSuperUserAccess();
        validateClusterExists(cluster);

        try {
            globalZk().delete(path("clusters", cluster, "domains", domainName), -1);
            // make sure that the cache content will be refreshed for the next read access
            domainCache().invalidate(domainName);
            // clear children cache and reload for further watch
            clusterDomainListCache().clear();
            clusterDomainListCache().get();
            //TODO: load-manager refresh brokerToDomainCache periodically
        } catch (KeeperException.NoNodeException nne) {
            log.warn("[{}] Domain {} does not exist in {}", clientAppId(), domainName, cluster);
            throw new RestException(Status.NOT_FOUND,
                    "Domain-name " + domainName + " or cluster " + cluster + " does not exist");
        } catch (Exception e) {
            log.error("[{}] Failed to delete domain {} in cluster {}", clientAppId(), domainName, cluster, e);
            throw new RestException(e);
        }
    }

    private void validateBrokerExistsInOtherDomain(final String cluster, final String inputDomainName,
            final Domain inputDomain) {
        if (inputDomain != null && inputDomain.brokers != null) {
            try {
                for (String domainName : clusterDomainListCache().get()) {
                    if (inputDomainName.equals(domainName)) {
                        continue;
                    }
                    try {
                        Optional<Domain> domain = domainCache().get(path("clusters", cluster, "domains", domainName));
                        if (domain.isPresent() && domain.get().brokers != null) {
                            List<String> duplicateBrokers = domain.get().brokers.stream().parallel()
                                    .filter(inputDomain.brokers::contains).collect(Collectors.toList());
                            if (!duplicateBrokers.isEmpty()) {
                                throw new RestException(Status.CONFLICT,
                                        duplicateBrokers + " already exist into " + domainName);
                            }
                        }
                    } catch (Exception e) {
                        if (e instanceof RestException) {
                            throw e;
                        }
                        log.warn("Failed to get domain {}", domainName, e);
                    }
                }
            } catch (KeeperException.NoNodeException e) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Domain is not configured for cluster", clientAppId(), e);
                }
            } catch (Exception e) {
                log.error("[{}] Failed to get domains for cluster {}", clientAppId(), e);
                throw new RestException(e);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Clusters.class);

}
