/*
 * Copyright The Cryostat Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cryostat.targets;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.ReflectionException;
import javax.management.remote.JMXServiceURL;

import org.openjdk.jmc.rjmx.ConnectionException;
import org.openjdk.jmc.rjmx.IConnectionHandle;
import org.openjdk.jmc.rjmx.ServiceNotAvailableException;

import io.cryostat.core.JvmIdentifier;
import io.cryostat.core.net.CryostatFlightRecorderService;
import io.cryostat.core.net.IDException;
import io.cryostat.core.net.JFRConnection;
import io.cryostat.core.net.MBeanMetrics;
import io.cryostat.core.sys.Clock;
import io.cryostat.core.templates.RemoteTemplateService;
import io.cryostat.core.templates.TemplateService;

import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

class AgentConnection implements JFRConnection {

    private final AgentClient client;
    private final Logger logger = Logger.getLogger(getClass());

    AgentConnection(AgentClient client) {
        this.client = client;
    }

    @Override
    public void close() throws Exception {}

    @Blocking
    @Override
    public void connect() throws ConnectionException {
        if (!client.ping().await().atMost(Duration.ofSeconds(10))) {
            throw new ConnectionException("Connection failed");
        }
    }

    @Override
    public void disconnect() {}

    public URI getUri() {
        return client.getUri();
    }

    @Override
    public long getApproximateServerTime(Clock clock) {
        return clock.now().toEpochMilli();
    }

    @Override
    public IConnectionHandle getHandle() throws ConnectionException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getHost() {
        return getUri().getHost();
    }

    public static boolean isAgentConnection(URI uri) {
        return Set.of("http", "https", "cryostat-agent").contains(uri.getScheme());
    }

    @Override
    public JMXServiceURL getJMXURL() throws IOException {
        if (!isAgentConnection(getUri())) {
            throw new UnsupportedOperationException();
        }
        return new JMXServiceURL(getUri().toString());
    }

    @Override
    public JvmIdentifier getJvmIdentifier() throws IDException, IOException {
        try {
            return JvmIdentifier.from(getMBeanMetrics().getRuntime());
        } catch (IntrospectionException | InstanceNotFoundException | ReflectionException e) {
            throw new IDException(e);
        }
    }

    @Override
    public int getPort() {
        return getUri().getPort();
    }

    @Override
    public CryostatFlightRecorderService getService()
            throws ConnectionException, IOException, ServiceNotAvailableException {
        return new AgentJFRService(client, getTemplateService());
    }

    @Override
    public TemplateService getTemplateService() {
        return new RemoteTemplateService(this);
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Blocking
    @Override
    public MBeanMetrics getMBeanMetrics()
            throws ConnectionException,
                    IOException,
                    InstanceNotFoundException,
                    IntrospectionException,
                    ReflectionException {
        return client.mbeanMetrics().await().atMost(Duration.ofSeconds(10));
    }

    @ApplicationScoped
    public static class Factory {

        @Inject AgentClient.Factory clientFactory;
        @Inject Logger logger;

        public AgentConnection createConnection(Target target) {
            return new AgentConnection(clientFactory.create(target));
        }
    }
}
