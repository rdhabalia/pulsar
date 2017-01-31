/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.admin;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Maps;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.web.NoSwaggerDocumentation;
import com.yahoo.pulsar.broker.web.PulsarWebResource;
import com.yahoo.pulsar.broker.web.RestException;
import com.yahoo.pulsar.common.configuration.FieldContext;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperCacheListener;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/v2/configuration/")
@NoSwaggerDocumentation
public class Configurations extends PulsarWebResource {

    public static final String BROKER_SERVICE_CONFIGURATION_PATH = "/loadbalance/configuration";

    private final ZooKeeperDataCache<Map<String, String>> dynamicConfigurationCache;
    private Map<String, String> configurationMap = null;

    public Configurations() throws Exception {
        this.dynamicConfigurationCache = new ZooKeeperDataCache<Map<String, String>>(pulsar().getLocalZkCache()) {
            @Override
            public Map<String, String> deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, HashMap.class);
            }
        };
        configurationMap = dynamicConfigurationCache.get(BROKER_SERVICE_CONFIGURATION_PATH).orElseGet(null);
        registerListenerToUpdateServiceConfiguration();
    }

    @PUT
    @Path("/{key}/{value}")
    @ApiOperation(value = "Update broker service configuration. This operation requires Pulsar super-user privileges.")
    @ApiResponses(value = { @ApiResponse(code = 204, message = "Service configuration updated successfully"),
            @ApiResponse(code = 403, message = "You don't have admin permission to update service-configuration") })
    public void updateConfiguration(@PathParam("key") String key, @PathParam("value") String value) {
        validateSuperUserAccess();
        boolean shouldZkUpdate = updateServiecConfiguration(key, value);
        if(shouldZkUpdate) {
            updateConfigurationOnZk(key, value);    
        }
    }

    private boolean updateServiecConfiguration(String key, String value) {
        try {
            Field field = ServiceConfiguration.class.getDeclaredField(key);
            if (field != null && field.isAnnotationPresent(FieldContext.class)) {
                field.setAccessible(true);
                field.set(pulsar().getConfiguration(), value);
                return ((FieldContext) field.getAnnotation(FieldContext.class)).updateOnZookeeper();
            }
        } catch (Exception e) {
            // TODO: logging
        }
        return false;
    }

    private void updateConfigurationOnZk(String key, String value) {
        try {
            Map<String, String> configurationMap = dynamicConfigurationCache.get(BROKER_SERVICE_CONFIGURATION_PATH)
                    .orElseGet(null);
            if (configurationMap != null) {
                configurationMap.put(key, value);
                byte[] content = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(configurationMap);
                dynamicConfigurationCache.invalidate(BROKER_SERVICE_CONFIGURATION_PATH);
                localZk().setData(BROKER_SERVICE_CONFIGURATION_PATH, content, -1);
            } else {
                configurationMap = Maps.newHashMap(key, value);
                byte[] content = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(configurationMap);
                ZkUtils.createFullPathOptimistic(localZk(), BROKER_SERVICE_CONFIGURATION_PATH, content,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            log.info("[{}] Updated Service configuration {}/{}", clientAppId(), key, value);
        } catch (Exception e) {
            log.error("[{}] Failed to update concurrent lookup permits {}", clientAppId(), e.getMessage(), e);
            throw new RestException(e);
        }

    }

    protected ZooKeeper localZk() {
        return pulsar().getZkClient();
    }

    private void registerListenerToUpdateServiceConfiguration() {
        dynamicConfigurationCache.registerListener(new ZooKeeperCacheListener<Map<String, String>>() {
            @Override
            public void onUpdate(String path, Map<String, String> data, Stat stat) {
                updateMapAndCallListener(path, data);
            }
        });
    }

    private synchronized void updateMapAndCallListener(String path, Map<String, String> data) {
        Set<String> diffKeys = data != null ? data.keySet() : null;
        if (configurationMap != null) {
            if (diffKeys != null) {
                diffKeys = getDiff(configurationMap, data);
                for (String key : diffKeys) {
                    String value = data.get(key);
                    try {
                        Method method = this.getClass().getDeclaredMethod(key, String.class);
                        if (method != null) {
                            method.invoke(this, value);
                        }
                    } catch (Exception e) {
                        // TODO: logging
                    }
                }
            }
        }
    }

    public static Set<String> getDiff(Map<String, String> mapA, Map<String, String> mapB) {
        Set<String> diff = mapA.keySet();
        for (String s : mapB.keySet()) {
            if (diff.contains(s) && (mapA.get(s).equals(mapB.get(s)))) {
                diff.remove(s);
            } else {
                diff.add(s);
            }
        }
        return diff;
    }

    // call this method from : updateMapAndCallListener() on config change using reflection
    private void maxConcurrentLookupRequest(String value) {
        if (StringUtils.isNotBlank(value)) {
            pulsar().getBrokerService().updateLookupRequestPermits(Integer.parseInt(value));
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Configurations.class);
}