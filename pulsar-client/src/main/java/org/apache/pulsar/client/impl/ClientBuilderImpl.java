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
package org.apache.pulsar.client.impl;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;

public class ClientBuilderImpl implements ClientBuilder {
    ClientConfigurationData conf;

    public ClientBuilderImpl() {
        this(new ClientConfigurationData());
    }

    public ClientBuilderImpl(ClientConfigurationData conf) {
        this.conf = conf;
    }

    @Override
    public PulsarClient build() throws PulsarClientException {
        if (StringUtils.isBlank(conf.getServiceUrl()) && conf.getServiceUrlProvider() == null) {
            throw new IllegalArgumentException("service URL or service URL provider needs to be specified on the ClientBuilder object.");
        }
        if (StringUtils.isNotBlank(conf.getServiceUrl()) && conf.getServiceUrlProvider() != null) {
            throw new IllegalArgumentException("Can only chose one way service URL or service URL provider.");
        }
        if (conf.getServiceUrlProvider() != null) {
            if (StringUtils.isBlank(conf.getServiceUrlProvider().getServiceUrl())) {
                throw new IllegalArgumentException("Cannot get service url from service url provider.");
            } else {
                conf.setServiceUrl(conf.getServiceUrlProvider().getServiceUrl());
            }
        }
        PulsarClient client = new PulsarClientImpl(conf);
        if (conf.getServiceUrlProvider() != null) {
            conf.getServiceUrlProvider().initialize(client);
        }
        return client;
    }

    @Override
    public ClientBuilder clone() {
        return new ClientBuilderImpl(conf.clone());
    }

    @Override
    public ClientBuilder loadConf(Map<String, Object> config) {
        conf = ConfigurationDataUtils.loadData(
            config, conf, ClientConfigurationData.class);
        return this;
    }

    @Override
    public ClientBuilder serviceUrl(String serviceUrl) {
        if (StringUtils.isBlank(serviceUrl)) {
            throw new IllegalArgumentException("Param serviceUrl must not be blank.");
        }
        conf.setServiceUrl(serviceUrl);
        if (!conf.isUseTls()) {
            enableTls(serviceUrl.startsWith("pulsar+ssl") || serviceUrl.startsWith("https"));
        }
        return this;
    }

    @Override
    public ClientBuilder serviceUrlProvider(ServiceUrlProvider serviceUrlProvider) {
        if (serviceUrlProvider == null) {
            throw new IllegalArgumentException("Param serviceUrlProvider must not be null.");
        }
        conf.setServiceUrlProvider(serviceUrlProvider);
        return this;
    }

    @Override
    public ClientBuilder authentication(Authentication authentication) {
        conf.setAuthentication(authentication);
        return this;
    }

    @Override
    public ClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        conf.setAuthentication(AuthenticationFactory.create(authPluginClassName, authParamsString));
        return this;
    }

    @Override
    public ClientBuilder authentication(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException {
        conf.setAuthentication(AuthenticationFactory.create(authPluginClassName, authParams));
        return this;
    }

    @Override
    public ClientBuilder operationTimeout(int operationTimeout, TimeUnit unit) {
        conf.setOperationTimeoutMs(unit.toMillis(operationTimeout));
        return this;
    }

    @Override
    public ClientBuilder ioThreads(int numIoThreads) {
        conf.setNumIoThreads(numIoThreads);
        return this;
    }

    @Override
    public ClientBuilder listenerThreads(int numListenerThreads) {
        conf.setNumListenerThreads(numListenerThreads);
        return this;
    }

    @Override
    public ClientBuilder connectionsPerBroker(int connectionsPerBroker) {
        conf.setConnectionsPerBroker(connectionsPerBroker);
        return this;
    }

    @Override
    public ClientBuilder enableTcpNoDelay(boolean useTcpNoDelay) {
        conf.setUseTcpNoDelay(useTcpNoDelay);
        return this;
    }

    @Override
    public ClientBuilder enableTls(boolean useTls) {
        conf.setUseTls(useTls);
        return this;
    }

    @Override
    public ClientBuilder enableTlsHostnameVerification(boolean enableTlsHostnameVerification) {
        conf.setTlsHostnameVerificationEnable(enableTlsHostnameVerification);
        return this;
    }

    @Override
    public ClientBuilder tlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        conf.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
        return this;
    }

    @Override
    public ClientBuilder allowTlsInsecureConnection(boolean tlsAllowInsecureConnection) {
        conf.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
        return this;
    }

    @Override
    public ClientBuilder statsInterval(long statsInterval, TimeUnit unit) {
        conf.setStatsIntervalSeconds(unit.toSeconds(statsInterval));
        return this;
    }

    @Override
    public ClientBuilder maxConcurrentLookupRequests(int concurrentLookupRequests) {
        conf.setConcurrentLookupRequest(concurrentLookupRequests);
        return this;
    }

    @Override
    public ClientBuilder maxLookupRequests(int maxLookupRequests) {
        conf.setMaxLookupRequest(maxLookupRequests);
        return this;
    }

    @Override
    public ClientBuilder maxNumberOfRejectedRequestPerConnection(int maxNumberOfRejectedRequestPerConnection) {
        conf.setMaxNumberOfRejectedRequestPerConnection(maxNumberOfRejectedRequestPerConnection);
        return this;
    }

    @Override
    public ClientBuilder keepAliveInterval(int keepAliveInterval, TimeUnit unit) {
        conf.setKeepAliveIntervalSeconds((int)unit.toSeconds(keepAliveInterval));
        return this;
    }

    @Override
    public ClientBuilder connectionTimeout(int duration, TimeUnit unit) {
        conf.setConnectionTimeoutMs((int)unit.toMillis(duration));
        return this;
    }

    @Override
    public ClientBuilder startingBackoffInterval(long duration, TimeUnit unit) {
    	conf.setInitialBackoffIntervalNanos(unit.toNanos(duration));
    	return this;
    }
    
    @Override
    public ClientBuilder maxBackoffInterval(long duration, TimeUnit unit) {
    	conf.setMaxBackoffIntervalNanos(unit.toNanos(duration));
    	return this;
    }
    
    public ClientConfigurationData getClientConfigurationData() {
        return conf;
    }

    @Override
    public ClientBuilder clock(Clock clock) {
        conf.setClock(clock);
        return this;
    }

    @Override
    public ClientBuilder proxyServiceUrl(String proxyServiceUrl, ProxyProtocol proxyProtocol) {
        if (StringUtils.isNotBlank(proxyServiceUrl) && proxyProtocol == null) {
            throw new IllegalArgumentException("proxyProtocol must be present with proxyServiceUrl");
        }
        conf.setProxyServiceUrl(proxyServiceUrl);
        conf.setProxyProtocol(proxyProtocol);
        return this;
    }
}
