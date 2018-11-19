package org.apache.pulsar;

import java.io.IOException;

import javax.naming.AuthenticationException;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;

public class SimpleAuthenticationProvider implements AuthenticationProvider {

    @Override
    public void close() throws IOException {
        // no-op
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        // No-op
    }

    @Override
    public String getAuthMethodName() {
        return "test";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        return authData.getCommandData() != null ? authData.getCommandData() : authData.getHttpHeader("user");
    }

}