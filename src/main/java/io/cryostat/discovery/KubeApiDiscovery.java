package io.cryostat.discovery;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

import io.cryostat.core.sys.FileSystem;

import com.google.common.base.Optional;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
class OpenShiftDiscovery extends KubeApiDiscovery {
    @ConfigProperty(name = "cryostat.discovery.openshift.enabled")
    boolean enabled;

    @Override
    boolean available() {
        return super.available() && client().supports(Route.class);
    }

    @Override
    OpenShiftClient client() {
        return kubeConfig.withOpenShift(super.client());
    }
}

@ApplicationScoped
public class KubeApiDiscovery {
    public static final String REALM = "KubernetesApi";

    public static final long ENDPOINTS_INFORMER_RESYNC_PERIOD = Duration.ofSeconds(30).toMillis();

    @Inject Logger logger;

    @Inject KubeConfig kubeConfig;

    @ConfigProperty(name = "cryostat.discovery.kubernetes.enabled")
    boolean enabled;

    // private Integer memoHash;
    // private EnvironmentNode memoTree;
    // private final Lazy<JFRConnectionToolkit> connectionToolkit;
    // private final Logger logger;
    private final Map<Triple<String, String, String>, Pair<HasMetadata, DiscoveryNode>>
            discoveryNodeCache = new ConcurrentHashMap<>();
    // private final Map<Triple<String, String, String>, Object> queryLocks =
    //         new ConcurrentHashMap<>();

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
                                                kubeConfig
                                                        .kubeClient()
                                                        .endpoints()
                                                        .inNamespace(ns)
                                                        .inform(
                                                                new EndpointsHandler(),
                                                                ENDPOINTS_INFORMER_RESYNC_PERIOD));
                                        logger.info(
                                                String.format(
                                                        "Started Endpoints SharedInformer for"
                                                                + " namespace \"{}\"",
                                                        ns));
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

        logger.info(String.format("Starting %s client", REALM));

        try {
            nsInformers.get(); // trigger lazy init
        } catch (ConcurrentException e) {
            throw new IllegalStateException(e);
        }
    }

    void onStop(@Observes ShutdownEvent evt) {
        if (!(enabled() && available())) {
            return;
        }
        logger.info(String.format("Shutting down %s client", REALM));
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

    // @Override
    // public List<ServiceRef> listDiscoverableServices() {
    //     try {
    //         return getAllServiceRefs();
    //     } catch (Exception e) {
    //         logger.warn(e);
    //         return Collections.emptyList();
    //     }
    // }

    // @Override
    // public EnvironmentNode getDiscoveryTree() {
    //     int currentHash = 0;
    //     HashCodeBuilder hcb = new HashCodeBuilder();
    //     Map<String, SharedIndexInformer<Endpoints>> informers = safeGetInformers();
    //     for (var informer : informers.values()) {
    //         List<Endpoints> store = informer.getStore().list();
    //         hcb.append(store.hashCode());
    //     }
    //     currentHash = hcb.build();
    //     if (Objects.equals(memoHash, currentHash)) {
    //         logger.trace("Using memoized discovery tree");
    //         return new EnvironmentNode(memoTree);
    //     }
    //     memoHash = currentHash;
    //     EnvironmentNode realmNode =
    //             new EnvironmentNode(REALM, BaseNodeType.REALM, Collections.emptyMap(), Set.of());
    //     informers
    //             .entrySet()
    //             .forEach(
    //                     entry -> {
    //                         var namespace = entry.getKey();
    //                         var store = entry.getValue().getStore().list();
    //                         EnvironmentNode nsNode =
    //                                 new EnvironmentNode(namespace, KubernetesNodeType.NAMESPACE);
    //                         try {
    //                             store.stream()
    //                                     .flatMap(endpoints ->
    // getTargetTuples(endpoints).stream())
    //                                     .forEach(tuple -> buildOwnerChain(nsNode, tuple));
    //                         } catch (Exception e) {
    //                             logger.warn(e);
    //                         } finally {
    //                             discoveryNodeCache.clear();
    //                             queryLocks.clear();
    //                         }
    //                         realmNode.addChildNode(nsNode);
    //                     });
    //     memoTree = realmNode;
    //     return realmNode;
    // }

    private Map<String, SharedIndexInformer<Endpoints>> safeGetInformers() {
        Map<String, SharedIndexInformer<Endpoints>> informers;
        try {
            informers = nsInformers.get();
        } catch (ConcurrentException e) {
            throw new IllegalStateException(e);
        }
        return informers;
    }

    // private void buildOwnerChain(EnvironmentNode nsNode, TargetTuple targetTuple) {
    //     ObjectReference target = targetTuple.addr.getTargetRef();
    //     if (target == null) {
    //         logger.error(
    //                 "Address {} for Endpoint {} had null target reference",
    //                 targetTuple.addr.getIp() != null
    //                         ? targetTuple.addr.getIp()
    //                         : targetTuple.addr.getHostname(),
    //                 targetTuple.objRef.getName());
    //         return;
    //     }
    //     String targetKind = target.getKind();
    //     KubernetesNodeType targetType = KubernetesNodeType.fromKubernetesKind(targetKind);
    //     if (targetType == KubernetesNodeType.POD) {
    //         // if the Endpoint points to a Pod, chase the owner chain up as far as possible, then
    //         // add that to the Namespace

    //         Pair<HasMetadata, EnvironmentNode> pod =
    //                 discoveryNodeCache.computeIfAbsent(
    //                         cacheKey(target.getNamespace(), target), this::queryForNode);
    //         pod.getRight()
    //                 .addChildNode(
    //                         new TargetNode(
    //                                 KubernetesNodeType.ENDPOINT, targetTuple.toServiceRef()));

    //         Pair<HasMetadata, EnvironmentNode> node = pod;
    //         while (true) {
    //             Pair<HasMetadata, EnvironmentNode> owner = getOrCreateOwnerNode(node);
    //             if (owner == null) {
    //                 break;
    //             }
    //             EnvironmentNode ownerNode = owner.getRight();
    //             ownerNode.addChildNode(node.getRight());
    //             node = owner;
    //         }
    //         nsNode.addChildNode(node.getRight());
    //     } else {
    //         // if the Endpoint points to something else(?) than a Pod, just add the target
    // straight
    //         // to the Namespace
    //         nsNode.addChildNode(
    //                 new TargetNode(KubernetesNodeType.ENDPOINT, targetTuple.toServiceRef()));
    //     }
    // }

    // private Pair<HasMetadata, EnvironmentNode> getOrCreateOwnerNode(
    //         Pair<HasMetadata, EnvironmentNode> child) {
    //     HasMetadata childRef = child.getLeft();
    //     if (childRef == null) {
    //         logger.error(
    //                 "Could not locate node named {} of kind {} while traversing environment",
    //                 child.getRight().getName(),
    //                 child.getRight().getNodeType());
    //         return null;
    //     }
    //     List<OwnerReference> owners = childRef.getMetadata().getOwnerReferences();
    //     // Take first "expected" owner Kind from NodeTypes, or if none, simply use the first
    // owner.
    //     // If there are no owners then return null to signify this and break the chain
    //     if (owners.isEmpty()) {
    //         return null;
    //     }
    //     String namespace = childRef.getMetadata().getNamespace();
    //     OwnerReference owner =
    //             owners.stream()
    //                     .filter(o -> KubernetesNodeType.fromKubernetesKind(o.getKind()) != null)
    //                     .findFirst()
    //                     .orElse(owners.get(0));
    //     return discoveryNodeCache.computeIfAbsent(cacheKey(namespace, owner),
    // this::queryForNode);
    // }

    private Triple<String, String, String> cacheKey(String ns, OwnerReference resource) {
        return Triple.of(ns, resource.getKind(), resource.getName());
    }

    // Unfortunately, ObjectReference and OwnerReference both independently implement getKind and
    // getName - they don't come from a common base class.
    private Triple<String, String, String> cacheKey(String ns, ObjectReference resource) {
        return Triple.of(ns, resource.getKind(), resource.getName());
    }

    // private Pair<HasMetadata, EnvironmentNode> queryForNode(
    //         Triple<String, String, String> lookupKey) {
    //     String namespace = lookupKey.getLeft();
    //     KubernetesNodeType nodeType =
    // KubernetesNodeType.fromKubernetesKind(lookupKey.getMiddle());
    //     String nodeName = lookupKey.getRight();
    //     if (nodeType == null) {
    //         return null;
    //     }
    //     synchronized (queryLocks.computeIfAbsent(lookupKey, k -> new Object())) {
    //         EnvironmentNode node;
    //         HasMetadata kubeObj =
    //
    // nodeType.getQueryFunction().apply(k8sClient).apply(namespace).apply(nodeName);
    //         if (kubeObj != null) {
    //             node = new EnvironmentNode(nodeName, nodeType,
    // kubeObj.getMetadata().getLabels());
    //         } else {
    //             node = new EnvironmentNode(nodeName, nodeType);
    //         }
    //         return Pair.of(kubeObj, node);
    //     }
    // }

    // private boolean isCompatiblePort(EndpointPort port) {
    //     return "jfr-jmx".equals(port.getName()) || 9091 == port.getPort();
    // }

    // private List<ServiceRef> getAllServiceRefs() {
    //     return safeGetInformers().values().stream()
    //             .flatMap(i -> i.getStore().list().stream())
    //             .flatMap(endpoints -> getServiceRefs(endpoints).stream())
    //             .collect(Collectors.toList());
    // }

    // private List<TargetTuple> getTargetTuples(Endpoints endpoints) {
    //     List<TargetTuple> tts = new ArrayList<>();
    //     for (EndpointSubset subset : endpoints.getSubsets()) {
    //         for (EndpointPort port : subset.getPorts()) {
    //             if (!isCompatiblePort(port)) {
    //                 continue;
    //             }
    //             for (EndpointAddress addr : subset.getAddresses()) {
    //                 tts.add(new TargetTuple(addr.getTargetRef(), addr, port));
    //             }
    //         }
    //     }
    //     return tts;
    // }

    // private List<ServiceRef> getServiceRefs(Endpoints endpoints) {
    //     return getTargetTuples(endpoints).stream()
    //             .map(TargetTuple::toServiceRef)
    //             .filter(Objects::nonNull)
    //             .collect(Collectors.toList());
    // }

    class KubeConfig {
        public static final String KUBERNETES_NAMESPACE_PATH =
                "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

        @Inject Logger logger;
        @Inject FileSystem fs;

        @ConfigProperty(name = "cryostat.k8s.namespaces")
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

        OpenShiftClient withOpenShift(KubernetesClient client) {
            return client.adapt(OpenShiftClient.class);
        }
    }

    private final class EndpointsHandler implements ResourceEventHandler<Endpoints> {

        @Override
        public void onAdd(Endpoints obj) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'onAdd'");
        }

        @Override
        public void onUpdate(Endpoints oldObj, Endpoints newObj) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'onUpdate'");
        }

        @Override
        public void onDelete(Endpoints obj, boolean deletedFinalStateUnknown) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'onDelete'");
        }
        //         @Override
        //         public void onAdd(Endpoints endpoints) {
        //             getServiceRefs(endpoints)
        //                     .forEach(serviceRef -> notifyAsyncTargetDiscovery(EventKind.FOUND,
        // serviceRef));
        //         }

        //         @Override
        //         public void onUpdate(Endpoints oldEndpoints, Endpoints newEndpoints) {
        //             Set<ServiceRef> previousRefs = new HashSet<>(getServiceRefs(oldEndpoints));
        //             Set<ServiceRef> currentRefs = new HashSet<>(getServiceRefs(newEndpoints));

        //             if (previousRefs.equals(currentRefs)) {
        //                 return;
        //             }

        //             ServiceRef.compare(previousRefs).to(currentRefs).updated().stream()
        //                     .forEach(sr -> notifyAsyncTargetDiscovery(EventKind.MODIFIED, sr));

        //             ServiceRef.compare(previousRefs).to(currentRefs).added().stream()
        //                     .forEach(sr -> notifyAsyncTargetDiscovery(EventKind.FOUND, sr));
        // `
        //             ServiceRef.compare(previousRefs).to(currentRefs).removed().stream()
        //                     .forEach(sr -> notifyAsyncTargetDiscovery(EventKind.LOST, sr));
        //         }

        //         @Override
        //         public void onDelete(Endpoints endpoints, boolean deletedFinalStateUnknown) {
        //             if (deletedFinalStateUnknown) {
        //                 logger.warn("Deleted final state unknown: {}", endpoints);
        //                 return;
        //             }
        //             getServiceRefs(endpoints)
        //                     .forEach(serviceRef -> notifyAsyncTargetDiscovery(EventKind.LOST,
        // serviceRef));
        //         }
    }

    // class TargetTuple {
    //     ObjectReference objRef;
    //     EndpointAddress addr;
    //     EndpointPort port;

    //     TargetTuple(ObjectReference objRef, EndpointAddress addr, EndpointPort port) {
    //         this.objRef = objRef;
    //         this.addr = addr;
    //         this.port = port;
    //     }

    //     Target toTarget() {
    //         Pair<HasMetadata, DiscoveryNode> node =
    //                 discoveryNodeCache.computeIfAbsent(
    //                         cacheKey(objRef.getNamespace(), objRef),
    //                         this::queryForNode);
    //         HasMetadata podRef = node.getLeft();
    //         if (node.getRight().getNodeType() != KubernetesNodeType.POD) {
    //             throw new IllegalStateException();
    //         }
    //         if (podRef == null) {
    //             throw new IllegalStateException();
    //         }
    //         try {
    //             String targetName = objRef.getName();

    //             String ip = addr.getIp().replaceAll("\\.", "-");
    //             String namespace = podRef.getMetadata().getNamespace();
    //             String host = String.format("%s.%s.pod", ip, namespace);

    //             JMXServiceURL jmxUrl =
    //                     new JMXServiceURL(
    //                             "rmi",
    //                             "",
    //                             0,
    //                             "/jndi/rmi://" + host + ':' + port.getPort() + "/jmxrmi");
    //             ServiceRef serviceRef =
    //                     new ServiceRef(null, URI.create(jmxUrl.toString()), targetName);
    //             serviceRef.setLabels(podRef.getMetadata().getLabels());
    //             serviceRef.setPlatformAnnotations(podRef.getMetadata().getAnnotations());
    //             serviceRef.setCryostatAnnotations(
    //                     Map.of(
    //                             AnnotationKey.REALM,
    //                             REALM,
    //                             AnnotationKey.HOST,
    //                             addr.getIp(),
    //                             AnnotationKey.PORT,
    //                             Integer.toString(port.getPort()),
    //                             AnnotationKey.NAMESPACE,
    //                             addr.getTargetRef().getNamespace(),
    //                             AnnotationKey.POD_NAME,
    //                             addr.getTargetRef().getName()));
    //             return serviceRef;
    //         } catch (Exception e) {
    //             logger.warn(e);
    //             return null;
    //         }
    //     }
    // }
}
