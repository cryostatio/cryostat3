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
package io.cryostat.discovery;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import javax.management.remote.JMXServiceURL;

import io.cryostat.ConfigProperties;
import io.cryostat.URIUtil;
import io.cryostat.core.net.JFRConnectionToolkit;
import io.cryostat.core.sys.FileSystem;
import io.cryostat.targets.Target;
import io.cryostat.targets.Target.Annotations;
import io.cryostat.targets.Target.EventKind;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.security.auth.module.UnixSystem;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.net.SocketAddress;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.ext.web.codec.BodyCodec;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
class PodmanDiscovery extends ContainerDiscovery {
    @ConfigProperty(name = "cryostat.discovery.podman.enabled")
    boolean enabled;

    @Override
    public String getRealm() {
        return "Podman";
    }

    @Override
    protected SocketAddress getSocket() {
        long uid = new UnixSystem().getUid();
        return SocketAddress.domainSocketAddress(
                String.format("/run/user/%d/podman/podman.sock", uid));
    }

    @Override
    protected String getContainersQueryURL() {
        return "http://d/v3.0.0/libpod/containers/json";
    }

    @Override
    protected String getContainerQueryURL(ContainerSpec spec) {
        return String.format("http://d/v3.0.0/libpod/containers/%s/json", spec.Id());
    }

    @Override
    protected boolean enabled() {
        return enabled;
    }
}

@ApplicationScoped
class DockerDiscovery extends ContainerDiscovery {
    @ConfigProperty(name = "cryostat.discovery.docker.enabled")
    boolean enabled;

    @Override
    public String getRealm() {
        return "Docker";
    }

    @Override
    protected SocketAddress getSocket() {
        return SocketAddress.domainSocketAddress(String.format("/var/run/docker.sock"));
    }

    @Override
    protected String getContainersQueryURL() {
        return "http://d/v1.42/containers/json";
    }

    @Override
    protected String getContainerQueryURL(ContainerSpec spec) {
        return String.format("http://d/v1.42/containers/%s/json", spec.Id());
    }

    @Override
    protected boolean enabled() {
        return enabled;
    }
}

public abstract class ContainerDiscovery {
    public static final String DISCOVERY_LABEL = "io.cryostat.discovery";
    public static final String JMX_URL_LABEL = "io.cryostat.jmxUrl";
    public static final String JMX_HOST_LABEL = "io.cryostat.jmxHost";
    public static final String JMX_PORT_LABEL = "io.cryostat.jmxPort";

    @Inject Logger logger;
    @Inject FileSystem fs;
    @Inject Vertx vertx;
    @Inject WebClient webClient;
    @Inject JFRConnectionToolkit connectionToolkit;
    @Inject ObjectMapper mapper;

    @ConfigProperty(name = ConfigProperties.CONTAINERS_POLL_PERIOD)
    Duration pollPeriod;

    @ConfigProperty(name = ConfigProperties.CONTAINERS_REQUEST_TIMEOUT)
    Duration requestTimeout;

    protected long timerId;

    protected final CopyOnWriteArrayList<ContainerSpec> containers = new CopyOnWriteArrayList<>();

    @Transactional
    void onStart(@Observes StartupEvent evt) {
        if (!enabled()) {
            return;
        }

        Path socketPath = Path.of(getSocket().path());
        if (!(fs.exists(socketPath) && fs.isReadable(socketPath))) {
            logger.errorv(
                    "{0} enabled but socket {1} is not accessible!",
                    getClass().getName(), socketPath);
            return;
        }
        logger.infov("{0} started", getClass().getName());

        DiscoveryNode universe = DiscoveryNode.getUniverse();
        if (DiscoveryNode.getRealm(getRealm()).isEmpty()) {
            DiscoveryPlugin plugin = new DiscoveryPlugin();
            DiscoveryNode node = DiscoveryNode.environment(getRealm(), DiscoveryNode.REALM);
            plugin.realm = node;
            plugin.builtin = true;
            universe.children.add(node);
            plugin.persist();
            universe.persist();
        }

        queryContainers();
        this.timerId = vertx.setPeriodic(pollPeriod.toMillis(), unused -> queryContainers());
    }

    void onStop(@Observes ShutdownEvent evt) {
        if (!enabled()) {
            return;
        }
        logger.info(String.format("Shutting down %s client", getRealm()));
        vertx.cancelTimer(timerId);
    }

    private void queryContainers() {
        doContainerListRequest(
                current -> {
                    Set<ContainerSpec> previous = new HashSet<>(containers);
                    Set<ContainerSpec> updated = new HashSet<>(current);

                    Set<ContainerSpec> intersection = new HashSet<>(containers);
                    intersection.retainAll(updated);

                    Set<ContainerSpec> removed = new HashSet<>(previous);
                    removed.removeAll(intersection);

                    Set<ContainerSpec> added = new HashSet<>(updated);
                    added.removeAll(intersection);

                    containers.removeAll(removed);
                    Infrastructure.getDefaultWorkerPool()
                            .execute(
                                    () ->
                                            removed.stream()
                                                    .filter(Objects::nonNull)
                                                    .forEach(
                                                            container ->
                                                                    handleContainerEvent(
                                                                            container,
                                                                            EventKind.LOST)));

                    containers.addAll(added);
                    Infrastructure.getDefaultWorkerPool()
                            .execute(
                                    () ->
                                            added.stream()
                                                    .filter(Objects::nonNull)
                                                    .forEach(
                                                            container ->
                                                                    handleContainerEvent(
                                                                            container,
                                                                            EventKind.FOUND)));
                });
    }

    private void doContainerListRequest(Consumer<List<ContainerSpec>> successHandler) {
        logger.trace(String.format("Shutting down %s client", getRealm()));
        URI requestPath = URI.create(getContainersQueryURL());
        try {
            webClient
                    .request(HttpMethod.GET, getSocket(), 80, "localhost", requestPath.toString())
                    .addQueryParam(
                            "filters",
                            mapper.writeValueAsString(Map.of("label", List.of(DISCOVERY_LABEL))))
                    .timeout(requestTimeout.toMillis())
                    .as(BodyCodec.string())
                    .send()
                    .subscribe()
                    .with(
                            item -> {
                                try {
                                    successHandler.accept(
                                            mapper.readValue(
                                                    item.body(),
                                                    new TypeReference<List<ContainerSpec>>() {}));
                                } catch (JsonProcessingException e) {
                                    logger.error("Json processing error", e);
                                }
                            },
                            failure -> {
                                logger.errorv(failure, "{0} API request failed", getRealm());
                            });
        } catch (JsonProcessingException e) {
            logger.error("Json processing error", e);
        }
    }

    private CompletableFuture<ContainerDetails> doContainerInspectRequest(ContainerSpec container) {
        CompletableFuture<ContainerDetails> result = new CompletableFuture<>();
        URI requestPath = URI.create(getContainerQueryURL(container));
        webClient
                .request(HttpMethod.GET, getSocket(), 80, "localhost", requestPath.toString())
                .timeout(requestTimeout.toMillis())
                .as(BodyCodec.string())
                .send()
                .subscribe()
                .with(
                        item -> {
                            try {
                                result.complete(
                                        mapper.readValue(item.body(), ContainerDetails.class));
                            } catch (JsonProcessingException e) {
                                logger.error("Json processing error", e);
                                result.completeExceptionally(e);
                            }
                        },
                        failure -> {
                            logger.errorv(failure, "{0} API request failed", getRealm());
                            result.completeExceptionally(failure);
                        });
        return result;
    }

    @Transactional
    public void handleContainerEvent(ContainerSpec desc, EventKind evtKind) {
        URI connectUrl;
        String hostname;
        int jmxPort;
        try {
            JMXServiceURL serviceUrl;
            URI rmiTarget;
            if (desc.Labels.containsKey(JMX_URL_LABEL)) {
                serviceUrl = new JMXServiceURL(desc.Labels.get(JMX_URL_LABEL));
                connectUrl = URI.create(serviceUrl.toString());
                try {
                    rmiTarget = URIUtil.getRmiTarget(serviceUrl);
                    hostname = rmiTarget.getHost();
                    jmxPort = rmiTarget.getPort();
                } catch (IllegalArgumentException e) {
                    hostname = serviceUrl.getHost();
                    jmxPort = serviceUrl.getPort();
                }
            } else {
                jmxPort = Integer.parseInt(desc.Labels.get(JMX_PORT_LABEL));
                hostname = desc.Labels.get(JMX_HOST_LABEL);
                if (hostname == null) {
                    try {
                        hostname =
                                doContainerInspectRequest(desc)
                                        .get(2, TimeUnit.SECONDS)
                                        .Config
                                        .Hostname;
                    } catch (InterruptedException | TimeoutException | ExecutionException e) {
                        containers.remove(desc);
                        logger.warnv(e, "Invalid {0} target observed", getRealm());
                        return;
                    }
                }
            }
            serviceUrl = connectionToolkit.createServiceURL(hostname, jmxPort);
            connectUrl = URI.create(serviceUrl.toString());
        } catch (MalformedURLException | URISyntaxException e) {
            containers.remove(desc);
            logger.warnv(e, "Invalid {0} target observed", getRealm());
            return;
        }

        DiscoveryNode realm = DiscoveryNode.getRealm(getRealm()).orElseThrow();

        if (evtKind == EventKind.FOUND) {
            Target target = new Target();
            target.activeRecordings = new ArrayList<>();
            target.connectUrl = connectUrl;
            target.alias = Optional.ofNullable(desc.Names.get(0)).orElse(desc.Id);
            target.labels = desc.Labels;
            target.annotations = new Annotations();
            target.annotations
                    .cryostat()
                    .putAll(
                            Map.of(
                                    "REALM", // AnnotationKey.REALM,
                                    getRealm(),
                                    "HOST", // AnnotationKey.HOST,
                                    hostname,
                                    "PORT", // "AnnotationKey.PORT,
                                    Integer.toString(jmxPort)));

            DiscoveryNode node = DiscoveryNode.target(target);
            target.discoveryNode = node;
            String podName = desc.PodName;
            if (StringUtils.isNotBlank(podName)) {
                DiscoveryNode pod = DiscoveryNode.environment(podName, DiscoveryNode.POD);
                if (!realm.children.contains(pod)) {
                    pod.children.add(node);
                    realm.children.add(pod);
                } else {
                    pod =
                            DiscoveryNode.getChild(
                                            realm,
                                            n ->
                                                    podName.equals(n.name)
                                                            && DiscoveryNode.POD.equals(n.nodeType))
                                    .orElseThrow();
                    pod.children.add(node);
                }
                pod.persist();
            } else {
                realm.children.add(node);
            }
            target.persist();
            node.persist();
            realm.persist();
        } else {
            Target t = Target.getTargetByConnectUrl(connectUrl);
            String podName = desc.PodName;
            if (StringUtils.isNotBlank(podName)) {
                DiscoveryNode pod = DiscoveryNode.environment(podName, DiscoveryNode.POD);
                pod.children.remove(t.discoveryNode);
            } else {
                realm.children.remove(t.discoveryNode);
            }
            realm.persist();
            t.delete();
        }
    }

    protected abstract SocketAddress getSocket();

    protected abstract String getRealm();

    protected abstract String getContainersQueryURL();

    protected abstract String getContainerQueryURL(ContainerSpec spec);

    protected abstract boolean enabled();

    static record PortSpec(
            long container_port, String host_ip, long host_port, String protocol, long range) {}

    static record ContainerSpec(
            String Id,
            String Image,
            Map<String, String> Labels,
            List<String> Names,
            long Pid,
            String Pod,
            String PodName,
            List<PortSpec> Ports,
            long StartedAt,
            String State) {}

    static record ContainerDetails(Config Config) {}

    static record Config(String Hostname) {}
}
