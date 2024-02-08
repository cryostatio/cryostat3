package io.cryostat.discovery;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import io.cryostat.core.sys.FileSystem;
import io.cryostat.targets.Target.EventKind;

import com.google.common.base.Optional;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointPort;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class KubeApiDiscovery {
    public static final String REALM = "KubernetesApi";

    public static final long ENDPOINTS_INFORMER_RESYNC_PERIOD = Duration.ofSeconds(30).toMillis();

    @Inject Logger logger;

    @Inject KubeConfig kubeConfig;

    @ConfigProperty(name = "cryostat.discovery.kubernetes.enabled")
    boolean enabled;

    @ConfigProperty(name = "cryostat.discovery.k8s.port-names")
    Optional<List<String>> JmxPortNames;

    @ConfigProperty(name = "cryostat.discovery.k8s.port-numbers")
    Optional<List<Integer>> JmxPortNumbers;

    private final LazyInitializer<HashMap<String, SharedIndexInformer<Endpoints>>> nsInformers =
            new LazyInitializer<HashMap<String, SharedIndexInformer<Endpoints>>>() {
                @Override
                protected HashMap<String, SharedIndexInformer<Endpoints>> initialize()
                        throws ConcurrentException {
                    // TODO add support for some wildcard indicating a single Informer for any
                    // namespace that Cryostat has permissions to. This will need some restructuring
                    // of how the
                    // namespaces within the discovery tree are mapped.
                    var result = new HashMap<String, SharedIndexInformer<Endpoints>>();
                    kubeConfig
                            .getWatchNamespaces()
                            .forEach(
                                    ns -> {
                                        result.put(
                                                ns,
                                                client().endpoints()
                                                        .inNamespace(ns)
                                                        .inform(
                                                                new EndpointsHandler(),
                                                                ENDPOINTS_INFORMER_RESYNC_PERIOD));
                                        logger.infov(
                                                "Started Endpoints SharedInformer for"
                                                        + " namespace \"{}\"",
                                                ns);
                                    });
                    return result;
                }
            };

    @Transactional
    void onStart(@Observes StartupEvent evt) {
        if (!(enabled())) {
            return;
        }

        if (!available()) {
            logger.errorv("{0} enabled but environment is not Kubernetes!", getClass().getName());
            return;
        }

        DiscoveryNode universe = DiscoveryNode.getUniverse();
        if (DiscoveryNode.getRealm(REALM).isEmpty()) {
            DiscoveryPlugin plugin = new DiscoveryPlugin();
            DiscoveryNode node = DiscoveryNode.environment(REALM, BaseNodeType.REALM);
            plugin.realm = node;
            plugin.builtin = true;
            universe.children.add(node);
            plugin.persist();
            universe.persist();
        }

        logger.infov("Starting %s client", REALM);

        safeGetInformers(); // trigger lazy init
    }

    void onStop(@Observes ShutdownEvent evt) {
        if (!(enabled() && available())) {
            return;
        }

        logger.infov("Shutting down %s client", REALM);
    }

    boolean enabled() {
        return enabled;
    }

    boolean available() {
        try {
            boolean hasNamespace = StringUtils.isNotBlank(kubeConfig.getOwnNamespace());
            return kubeConfig.kubeApiAvailable() || hasNamespace;
        } catch (Exception e) {
            logger.info(e);
        }
        return false;
    }

    KubernetesClient client() {
        return kubeConfig.kubeClient();
    }

    private Map<String, SharedIndexInformer<Endpoints>> safeGetInformers() {
        Map<String, SharedIndexInformer<Endpoints>> informers;
        try {
            informers = nsInformers.get();
        } catch (ConcurrentException e) {
            throw new IllegalStateException(e);
        }
        return informers;
    }

    private boolean isCompatiblePort(EndpointPort port) {
        return JmxPortNames.or(List.of()).contains(port.getName())
                || JmxPortNumbers.or(List.of()).contains(port.getPort());
    }

    private List<TargetRef> getTargetRefsFrom(Endpoints endpoints) {
        return TargetRef.fromEndpoints(endpoints).stream()
                .filter(
                        (ref) -> {
                            return Objects.nonNull(ref) && isCompatiblePort(ref.port());
                        })
                .collect(Collectors.toList());
    }

    @Transactional
    public void handleEndpointEvent(TargetRef ref, EventKind eventKind) {
        // TODO: Handle endpoint event
    }

    @ApplicationScoped
    static final class KubeConfig {
        public static final String KUBERNETES_NAMESPACE_PATH =
                "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

        @Inject Logger logger;
        @Inject FileSystem fs;

        @ConfigProperty(name = "cryostat.discovery.k8s.namespaces")
        Optional<List<String>> watchNamespaces;

        @ConfigProperty(name = "kubernetes.service.host")
        Optional<String> serviceHost;

        private KubernetesClient kubeClient;

        List<String> getWatchNamespaces() {
            return watchNamespaces.or(List.of());
        }

        String getOwnNamespace() {
            try {
                return fs.readString(Path.of(KUBERNETES_NAMESPACE_PATH));
            } catch (Exception e) {
                logger.trace(e);
                return null;
            }
        }

        boolean kubeApiAvailable() {
            return serviceHost.isPresent();
        }

        KubernetesClient kubeClient() {
            if (kubeClient == null) {
                kubeClient =
                        new KubernetesClientBuilder()
                                .withTaskExecutor(ForkJoinPool.commonPool())
                                .build();
            }
            return kubeClient;
        }
    }

    private final class EndpointsHandler implements ResourceEventHandler<Endpoints> {
        @Override
        public void onAdd(Endpoints endpoints) {
            getTargetRefsFrom(endpoints)
                    .forEach(
                            (refs) -> {
                                handleEndpointEvent(refs, EventKind.FOUND);
                            });
        }

        @Override
        public void onUpdate(Endpoints oldEndpoints, Endpoints newEndpoints) {
            Set<TargetRef> previousRefs = new HashSet<>(getTargetRefsFrom(oldEndpoints));
            Set<TargetRef> currentRefs = new HashSet<>(getTargetRefsFrom(newEndpoints));

            if (previousRefs.equals(currentRefs)) {
                return;
            }

            TargetRef.compare(previousRefs).to(currentRefs).updated().stream()
                    .forEach(ref -> handleEndpointEvent(ref, EventKind.MODIFIED));

            TargetRef.compare(previousRefs).to(currentRefs).added().stream()
                    .forEach(ref -> handleEndpointEvent(ref, EventKind.FOUND));

            TargetRef.compare(previousRefs).to(currentRefs).removed().stream()
                    .forEach(ref -> handleEndpointEvent(ref, EventKind.LOST));
        }

        @Override
        public void onDelete(Endpoints endpoints, boolean deletedFinalStateUnknown) {
            if (deletedFinalStateUnknown) {
                logger.warnv("Deleted final state unknown: {}", endpoints);
                return;
            }
            getTargetRefsFrom(endpoints)
                    .forEach(
                            (tt) -> {
                                handleEndpointEvent(tt, EventKind.LOST);
                            });
        }
    }

    static record TargetRef(ObjectReference objRef, EndpointAddress addr, EndpointPort port) {
        TargetRef {
            Objects.requireNonNull(objRef);
            Objects.requireNonNull(addr);
            Objects.requireNonNull(port);
        }

        static List<TargetRef> fromEndpoints(Endpoints endpoints) {
            List<TargetRef> tts = new ArrayList<>();
            for (EndpointSubset subset : endpoints.getSubsets()) {
                for (EndpointPort port : subset.getPorts()) {
                    for (EndpointAddress addr : subset.getAddresses()) {
                        tts.add(new TargetRef(addr.getTargetRef(), addr, port));
                    }
                }
            }
            return tts;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (other == this) {
                return true;
            }
            if (!(other instanceof TargetRef)) {
                return false;
            }
            TargetRef sr = (TargetRef) other;
            return new EqualsBuilder()
                    .append(objRef().getName(), objRef().getName())
                    .append(addr(), sr.addr())
                    .append(port(), sr.port())
                    .build();
        }

        public static Compare compare(Collection<TargetRef> src) {
            return new Compare(src);
        }

        public static class Compare {
            private Collection<TargetRef> previous, current;

            public Compare(Collection<TargetRef> previous) {
                this.previous = new HashSet<>(previous);
            }

            public Compare to(Collection<TargetRef> current) {
                this.current = new HashSet<>(current);
                return this;
            }

            public Collection<TargetRef> added() {
                return removeAllUpdatedRefs(addedOrUpdatedRefs(), updated());
            }

            public Collection<TargetRef> removed() {
                return removeAllUpdatedRefs(removedOrUpdatedRefs(), updated());
            }

            public Collection<TargetRef> updated() {
                Collection<TargetRef> updated = addedOrUpdatedRefs();
                updated.removeAll(removedOrUpdatedRefs());
                return updated;
            }

            private Collection<TargetRef> addedOrUpdatedRefs() {
                Collection<TargetRef> added = new HashSet<>(current);
                added.removeAll(previous);
                return added;
            }

            private Collection<TargetRef> removedOrUpdatedRefs() {
                Collection<TargetRef> removed = new HashSet<>(previous);
                removed.removeAll(current);
                return removed;
            }

            private Collection<TargetRef> removeAllUpdatedRefs(
                    Collection<TargetRef> src, Collection<TargetRef> updated) {
                Collection<TargetRef> tnSet = new HashSet<>(src);
                tnSet.removeAll(updated);
                return tnSet;
            }
        }
    }
}
