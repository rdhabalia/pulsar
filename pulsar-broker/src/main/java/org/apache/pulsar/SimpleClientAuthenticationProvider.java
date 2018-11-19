package org.apache.pulsar;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SimpleClientAuthenticationProvider implements Authentication {
    String user;

    public SimpleClientAuthenticationProvider() {
    }
    
    public SimpleClientAuthenticationProvider(String user) {
        this.user = user;
    }

    @Override
    public void close() throws IOException {
        // No-op
    }

    @Override
    public String getAuthMethodName() {
        return "test";
    }

    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        AuthenticationDataProvider provider = new AuthenticationDataProvider() {
            public boolean hasDataForHttp() {
                return true;
            }

            @SuppressWarnings("unchecked")
            public Set<Map.Entry<String, String>> getHttpHeaders() {
                return Sets.newHashSet(Maps.immutableEntry("user", user));
            }

            public boolean hasDataFromCommand() {
                return true;
            }

            public String getCommandData() {
                return user;
            }
        };
        return provider;
    }

    @Override
    public void configure(Map<String, String> authParams) {
        this.user = authParams.get("user");
    }

    @Override
    public void start() throws PulsarClientException {
        // No-op
    }

}
