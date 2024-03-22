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
package io.cryostat.recordings;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

import org.openjdk.jmc.common.unit.UnitLookup;
import org.openjdk.jmc.rjmx.services.jfr.IRecordingDescriptor;

import io.cryostat.recordings.Recordings.ArchivedRecording;
import io.cryostat.recordings.Recordings.LinkedRecordingDescriptor;
import io.cryostat.recordings.Recordings.Metadata;
import io.cryostat.targets.Target;
import io.cryostat.targets.TargetConnectionManager;
import io.cryostat.ws.MessagingServer;
import io.cryostat.ws.Notification;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import io.smallrye.common.annotation.Blocking;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PostRemove;
import jakarta.persistence.PostUpdate;
import jakarta.persistence.PreRemove;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.transaction.Transactional;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jdk.jfr.RecordingState;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.jboss.logging.Logger;

@Entity
@EntityListeners(ActiveRecording.Listener.class)
@Table(
        uniqueConstraints = {
            @UniqueConstraint(columnNames = {"target_id", "name"}),
            @UniqueConstraint(columnNames = {"target_id", "remoteId"})
        })
public class ActiveRecording extends PanacheEntity {

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "target_id")
    @NotNull
    public Target target;

    @NotBlank public String name;

    @PositiveOrZero public long remoteId;
    @NotNull public RecordingState state;
    @PositiveOrZero public long duration;
    @PositiveOrZero public long startTime;
    public boolean continuous;
    public boolean toDisk;
    @PositiveOrZero public long maxSize;
    @PositiveOrZero public long maxAge;

    @JdbcTypeCode(SqlTypes.JSON)
    @NotNull
    public Metadata metadata;

    public static ActiveRecording from(Target target, LinkedRecordingDescriptor descriptor) {
        ActiveRecording recording = new ActiveRecording();

        recording.target = target;
        recording.remoteId = descriptor.id();
        recording.name = descriptor.name();
        recording.state = RecordingState.RUNNING;
        recording.duration = descriptor.duration();
        recording.startTime = descriptor.startTime();
        recording.continuous = descriptor.continuous();
        recording.toDisk = descriptor.toDisk();
        recording.maxSize = descriptor.maxSize();
        recording.maxAge = descriptor.maxAge();
        recording.metadata = descriptor.metadata();

        return recording;
    }

    public static ActiveRecording from(
            Target target, IRecordingDescriptor descriptor, Metadata metadata) {
        ActiveRecording recording = new ActiveRecording();

        recording.target = target;
        recording.remoteId = descriptor.getId();
        recording.name = descriptor.getName();
        switch (descriptor.getState()) {
            case CREATED:
                recording.state = RecordingState.DELAYED;
                break;
            case RUNNING:
                recording.state = RecordingState.RUNNING;
                break;
            case STOPPING:
                recording.state = RecordingState.RUNNING;
                break;
            case STOPPED:
                recording.state = RecordingState.STOPPED;
                break;
            default:
                recording.state = RecordingState.NEW;
                break;
        }
        recording.duration = descriptor.getDuration().in(UnitLookup.MILLISECOND).longValue();
        recording.startTime = descriptor.getStartTime().in(UnitLookup.EPOCH_MS).longValue();
        recording.continuous = descriptor.isContinuous();
        recording.toDisk = descriptor.getToDisk();
        recording.maxSize = descriptor.getMaxSize().in(UnitLookup.BYTE).longValue();
        recording.maxAge = descriptor.getMaxAge().in(UnitLookup.MILLISECOND).longValue();
        recording.metadata = new Metadata(metadata);

        return recording;
    }

    public static ActiveRecording getByName(String name) {
        return find("name", name).singleResult();
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    @Transactional
    public static boolean deleteFromTarget(Target target, String recordingName) {
        Optional<ActiveRecording> recording =
                target.activeRecordings.stream()
                        .filter(r -> r.name.equals(recordingName))
                        .findFirst();
        boolean found = recording.isPresent();
        if (found) {
            Logger.getLogger(ActiveRecording.class)
                    .debugv("Found and deleting match: {0} / {1}", target.alias, recording.get());
            recording.get().delete();
            getEntityManager().flush();
        } else {
            Logger.getLogger(ActiveRecording.class)
                    .debugv(
                            "No match found for recording {0} in target {1}",
                            recordingName, target.alias);
        }
        return found;
    }

    @ApplicationScoped
    static class Listener {

        @Inject Logger logger;
        @Inject EventBus bus;
        @Inject TargetConnectionManager connectionManager;
        @Inject RecordingHelper recordingHelper;

        @PostPersist
        public void postPersist(ActiveRecording activeRecording) {
            bus.publish(
                    Recordings.RecordingEventCategory.ACTIVE_CREATED.category(), activeRecording);
            notify(
                    new ActiveRecordingEvent(
                            Recordings.RecordingEventCategory.ACTIVE_CREATED,
                            ActiveRecordingEvent.Payload.of(recordingHelper, activeRecording)));
        }

        @PreUpdate
        @Blocking
        public void preUpdate(ActiveRecording activeRecording) throws Exception {
            if (RecordingState.STOPPED.equals(activeRecording.state)) {
                try {
                    connectionManager.executeConnectedTask(
                            activeRecording.target,
                            conn -> {
                                RecordingHelper.getDescriptorById(conn, activeRecording.remoteId)
                                        .ifPresent(
                                                d -> {
                                                    // this connection can fail if we are removing
                                                    // this recording as a cascading operation after
                                                    // the owner target was lost. It isn't too
                                                    // important in that case that we are unable to
                                                    // connect to the target and close the actual
                                                    // recording, because the target probably went
                                                    // offline or we otherwise just can't reach it.
                                                    try {
                                                        if (!d.getState()
                                                                .equals(
                                                                        IRecordingDescriptor
                                                                                .RecordingState
                                                                                .STOPPED)) {
                                                            conn.getService().stop(d);
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                });
                                return null;
                            });
                } catch (Exception e) {
                    logger.error("Failed to stop remote recording", e);
                }
            }
        }

        @PostUpdate
        public void postUpdate(ActiveRecording activeRecording) {
            if (RecordingState.STOPPED.equals(activeRecording.state)) {
                bus.publish(
                        Recordings.RecordingEventCategory.ACTIVE_STOPPED.category(),
                        activeRecording);
                notify(
                        new ActiveRecordingEvent(
                                Recordings.RecordingEventCategory.ACTIVE_STOPPED,
                                ActiveRecordingEvent.Payload.of(recordingHelper, activeRecording)));
            }
        }

        @PreRemove
        @Blocking
        public void preRemove(ActiveRecording activeRecording) throws Exception {
            try {
                activeRecording.target.activeRecordings.remove(activeRecording);
                connectionManager.executeConnectedTask(
                        activeRecording.target,
                        conn -> {
                            // this connection can fail if we are removing this recording as a
                            // cascading operation after the owner target was lost. It isn't too
                            // important in that case that we are unable to connect to the target
                            // and close the actual recording, because the target probably went
                            // offline or we otherwise just can't reach it.
                            try {
                                RecordingHelper.getDescriptor(conn, activeRecording)
                                        .ifPresent(
                                                rec ->
                                                        Recordings.safeCloseRecording(
                                                                conn, rec, logger));
                            } catch (Exception e) {
                                logger.info(e);
                            }
                            return null;
                        });
            } catch (Exception e) {
                logger.error(e);
                throw e;
            }
        }

        @PostRemove
        public void postRemove(ActiveRecording activeRecording) {
            bus.publish(
                    Recordings.RecordingEventCategory.ACTIVE_DELETED.category(), activeRecording);
            notify(
                    new ActiveRecordingEvent(
                            Recordings.RecordingEventCategory.ACTIVE_DELETED,
                            ActiveRecordingEvent.Payload.of(recordingHelper, activeRecording)));
        }

        private void notify(ActiveRecordingEvent event) {
            bus.publish(
                    MessagingServer.class.getName(),
                    new Notification(event.category().category(), event.payload()));
        }

        public record ActiveRecordingEvent(
                Recordings.RecordingEventCategory category, Payload payload) {
            public ActiveRecordingEvent {
                Objects.requireNonNull(category);
                Objects.requireNonNull(payload);
            }

            public record Payload(String target, LinkedRecordingDescriptor recording) {
                public Payload {
                    Objects.requireNonNull(target);
                    Objects.requireNonNull(recording);
                }

                public static Payload of(RecordingHelper helper, ActiveRecording recording) {
                    return new Payload(
                            recording.target.connectUrl.toString(),
                            helper.toExternalForm(recording));
                }
            }
        }

        public record ArchivedRecordingEvent(
                Recordings.RecordingEventCategory category, Payload payload) {
            public ArchivedRecordingEvent {
                Objects.requireNonNull(category);
                Objects.requireNonNull(payload);
            }

            // FIXME the target connectUrl URI may no longer be known if the target
            // has disappeared and we are emitting an event regarding an archived recording
            // originally sourced from that target, or if we are accepting a recording upload from a
            // client.
            // This should embed the target jvmId and optionally the database ID.
            public record Payload(String target, ArchivedRecording recording) {
                public Payload {
                    Objects.requireNonNull(recording);
                }

                public static Payload of(URI connectUrl, ArchivedRecording recording) {
                    return new Payload(
                            Optional.ofNullable(connectUrl).map(URI::toString).orElse(null),
                            recording);
                }
            }
        }
    }
}
