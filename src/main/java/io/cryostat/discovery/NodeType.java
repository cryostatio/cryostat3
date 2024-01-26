package io.cryostat.discovery;

import java.util.function.Function;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;

public interface NodeType {
    String getKind();

    int ordinal();
}

enum BaseNodeType implements NodeType {
    // represents the entire deployment scenario Cryostat finds itself in
    UNIVERSE("Universe"),
    // represents a division of the deployment scenario - the universe may consist of a
    // Kubernetes Realm and a JDP Realm, for example
    REALM("Realm"),
    // represents a plain target JVM, connectable over JMX
    JVM("JVM"),
    // represents a target JVM using the Cryostat Agent, *not* connectable over JMX. Agent instances
    // that do publish a JMX Service URL should publish themselves with the JVM NodeType.
    AGENT("CryostatAgent"),
    ;

    private final String kind;

    BaseNodeType(String kind) {
        this.kind = kind;
    }

    @Override
    public String getKind() {
        return kind;
    }

    @Override
    public String toString() {
        return getKind();
    }
}

enum KubeDiscoveryNodeType implements NodeType {
    NAMESPACE("Namespace"),
    STATEFULSET(
            "StatefulSet",
            c -> ns -> n -> c.apps().statefulSets().inNamespace(ns).withName(n).get()),
    DAEMONSET("DaemonSet", c -> ns -> n -> c.apps().daemonSets().inNamespace(ns).withName(n).get()),
    DEPLOYMENT(
            "Deployment", c -> ns -> n -> c.apps().deployments().inNamespace(ns).withName(n).get()),
    REPLICASET(
            "ReplicaSet", c -> ns -> n -> c.apps().replicaSets().inNamespace(ns).withName(n).get()),
    REPLICATIONCONTROLLER(
            "ReplicationController",
            c -> ns -> n -> c.replicationControllers().inNamespace(ns).withName(n).get()),
    POD("Pod", c -> ns -> n -> c.pods().inNamespace(ns).withName(n).get()),
    ENDPOINT("Endpoint", c -> ns -> n -> c.endpoints().inNamespace(ns).withName(n).get()),
    // OpenShift resources
    DEPLOYMENTCONFIG("DeploymentConfig"),
    ;

    private final String kubernetesKind;
    private final transient Function<
                    KubernetesClient, Function<String, Function<String, ? extends HasMetadata>>>
            getFn;

    KubeDiscoveryNodeType(String kubernetesKind) {
        this(kubernetesKind, client -> namespace -> name -> null);
    }

    KubeDiscoveryNodeType(
            String kubernetesKind,
            Function<KubernetesClient, Function<String, Function<String, ? extends HasMetadata>>>
                    getFn) {
        this.kubernetesKind = kubernetesKind;
        this.getFn = getFn;
    }

    @Override
    public String getKind() {
        return kubernetesKind;
    }

    public Function<KubernetesClient, Function<String, Function<String, ? extends HasMetadata>>>
            getQueryFunction() {
        return getFn;
    }

    public static KubeDiscoveryNodeType fromKubernetesKind(String kubernetesKind) {
        if (kubernetesKind == null) {
            return null;
        }
        for (KubeDiscoveryNodeType nt : values()) {
            if (kubernetesKind.equalsIgnoreCase(nt.kubernetesKind)) {
                return nt;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return getKind();
    }
}

enum ContainerDiscoveryNodeType implements NodeType {
    // represents a container pod managed by Podman
    POD("Pod"),
    ;

    private final String kind;

    ContainerDiscoveryNodeType(String kind) {
        this.kind = kind;
    }

    @Override
    public String getKind() {
        return kind;
    }

    @Override
    public String toString() {
        return getKind();
    }
}
