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

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.cryostat.ConfigProperties;
import io.cryostat.core.JvmIdentifier;
import io.cryostat.core.net.JFRConnection;
import io.cryostat.credentials.Credential;
import io.cryostat.discovery.DiscoveryNode;
import io.cryostat.expressions.MatchExpressionEvaluator;
import io.cryostat.recordings.ActiveRecording;
import io.cryostat.recordings.RecordingHelper;
import io.cryostat.ws.MessagingServer;
import io.cryostat.ws.Notification;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.quarkus.hibernate.orm.panache.PanacheEntity;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.common.annotation.Blocking;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PostRemove;
import jakarta.persistence.PostUpdate;
import jakarta.persistence.PrePersist;
import jakarta.transaction.Transactional;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.jboss.logging.Logger;
import org.projectnessie.cel.tools.ScriptException;

@Entity
@EntityListeners(Target.Listener.class)
public class Target extends PanacheEntity {

    public static final String TARGET_JVM_DISCOVERY = "TargetJvmDiscovery";

    @Column(unique = true, updatable = false)
    @NotNull
    public URI connectUrl;

    @NotBlank public String alias;

    public String jvmId;

    @JdbcTypeCode(SqlTypes.JSON)
    @NotNull
    public Map<String, String> labels = new HashMap<>();

    @JdbcTypeCode(SqlTypes.JSON)
    @NotNull
    public Annotations annotations = new Annotations();

    @OneToMany(
            mappedBy = "target",
            cascade = {CascadeType.ALL},
            orphanRemoval = true)
    @NotNull
    @JsonIgnore
    public List<ActiveRecording> activeRecordings = new ArrayList<>();

    @OneToOne(
            cascade = {CascadeType.ALL},
            orphanRemoval = true)
    @JoinColumn(name = "discoveryNode")
    @NotNull
    @JsonIgnore
    public DiscoveryNode discoveryNode;

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public boolean isAgent() {
        return AgentConnection.isAgentConnection(connectUrl);
    }

    @JsonIgnore
    public String targetId() {
        return this.connectUrl.toString();
    }

    public static Target getTargetById(long targetId) {
        return Target.find("id", targetId).singleResult();
    }

    public static Target getTargetByConnectUrl(URI connectUrl) {
        return find("connectUrl", connectUrl).singleResult();
    }

    public static Optional<Target> getTargetByJvmId(String jvmId) {
        return find("jvmId", jvmId).firstResultOptional();
    }

    public static boolean deleteByConnectUrl(URI connectUrl) {
        return delete("connectUrl", connectUrl) > 0;
    }

    public ActiveRecording getRecordingById(long remoteId) {
        return activeRecordings.stream()
                .filter(rec -> rec.remoteId == remoteId)
                .findFirst()
                .orElse(null);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public static record Annotations(Map<String, String> platform, Map<String, String> cryostat) {
        public Annotations {
            if (platform == null) {
                platform = new HashMap<>();
            }
            if (cryostat == null) {
                cryostat = new HashMap<>();
            }
        }

        public Annotations() {
            this(new HashMap<>(), new HashMap<>());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, annotations, connectUrl, jvmId, labels);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Target other = (Target) obj;
        return Objects.equals(alias, other.alias)
                && Objects.equals(annotations, other.annotations)
                && Objects.equals(connectUrl, other.connectUrl)
                && Objects.equals(jvmId, other.jvmId)
                && Objects.equals(labels, other.labels);
    }

    public enum EventKind {
        FOUND,
        MODIFIED,
        LOST,
        ;
    }

    @SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
    public record TargetDiscovery(EventKind kind, Target serviceRef) {
        public TargetDiscovery {
            Objects.requireNonNull(kind);
            Objects.requireNonNull(serviceRef);
        }
    }

    @ApplicationScoped
    static class Listener {

        @Inject Logger logger;
        @Inject EventBus bus;
        @Inject TargetConnectionManager connectionManager;
        @Inject RecordingHelper recordingHelper;
        @Inject MatchExpressionEvaluator matchExpressionEvaluator;

        @ConfigProperty(name = ConfigProperties.CONNECTIONS_FAILED_TIMEOUT)
        Duration timeout;

        @Transactional
        @ConsumeEvent(value = Target.TARGET_JVM_DISCOVERY, blocking = true)
        void onMessage(TargetDiscovery event) {
            var target = Target.<Target>find("id", event.serviceRef().id).singleResultOptional();
            switch (event.kind()) {
                case LOST:
                    // this should already be handled by the cascading deletion of the Target
                    // TODO verify this
                    break;
                case FOUND:
                    target.ifPresent(recordingHelper::updateRemoteRecordings);
                    break;
                case MODIFIED:
                    target.ifPresent(recordingHelper::updateRemoteRecordings);
                    break;
                default:
                    // no-op
                    break;
            }
        }

        @ConsumeEvent(value = Credential.CREDENTIALS_STORED, blocking = true)
        @Transactional
        void updateCredential(Credential credential) {
            Target.<Target>find("jvmId", (String) null)
                    .list()
                    .forEach(
                            t -> {
                                try {
                                    if (matchExpressionEvaluator.applies(
                                            credential.matchExpression, t)) {
                                        updateTargetJvmId(t, credential);
                                        t.persist();
                                    }
                                } catch (ScriptException e) {
                                    logger.error(e);
                                } catch (Exception e) {
                                    logger.warn(e);
                                }
                            });
        }

        @Blocking
        @PrePersist
        void prePersist(Target target) {
            if (StringUtils.isBlank(target.alias)) {
                throw new IllegalArgumentException();
            }
            var encodedAlias = URLEncoder.encode(target.alias, StandardCharsets.UTF_8);
            if (!Objects.equals(encodedAlias, target.alias)) {
                target.alias = encodedAlias;
            }

            try {
                if (StringUtils.isBlank(target.jvmId)) {
                    updateTargetJvmId(target, null);
                }
            } catch (Exception e) {
                logger.info(e);
            }
        }

        @Blocking
        private void updateTargetJvmId(Target t, Credential credential) {
            t.jvmId =
                    connectionManager
                            .executeDirect(
                                    t,
                                    Optional.ofNullable(credential),
                                    JFRConnection::getJvmIdentifier)
                            .map(JvmIdentifier::getHash)
                            .await()
                            .atMost(timeout);
        }

        @PostPersist
        void postPersist(Target target) {
            notify(EventKind.FOUND, target);
        }

        @PostUpdate
        void postUpdate(Target target) {
            notify(EventKind.MODIFIED, target);
        }

        @PostRemove
        void postRemove(Target target) {
            notify(EventKind.LOST, target);
        }

        private void notify(EventKind eventKind, Target target) {
            bus.publish(
                    MessagingServer.class.getName(),
                    new Notification(
                            TARGET_JVM_DISCOVERY,
                            new TargetDiscoveryEvent(new TargetDiscovery(eventKind, target))));
            bus.publish(TARGET_JVM_DISCOVERY, new TargetDiscovery(eventKind, target));
        }

        public record TargetDiscoveryEvent(TargetDiscovery event) {
            public TargetDiscoveryEvent {
                Objects.requireNonNull(event);
            }
        }
    }
}
