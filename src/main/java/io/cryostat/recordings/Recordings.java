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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openjdk.jmc.common.unit.IConstrainedMap;
import org.openjdk.jmc.common.unit.IOptionDescriptor;
import org.openjdk.jmc.flightrecorder.configuration.IFlightRecorderService;
import org.openjdk.jmc.flightrecorder.configuration.IRecordingDescriptor;
import org.openjdk.jmc.flightrecorder.configuration.recording.RecordingOptionsBuilder;

import io.cryostat.ConfigProperties;
import io.cryostat.Producers;
import io.cryostat.StorageBuckets;
import io.cryostat.V2Response;
import io.cryostat.core.EventOptionsBuilder;
import io.cryostat.core.RecordingOptionsCustomizer;
import io.cryostat.core.sys.Clock;
import io.cryostat.core.templates.Template;
import io.cryostat.core.templates.TemplateType;
import io.cryostat.recordings.ActiveRecording.Listener.ArchivedRecordingEvent;
import io.cryostat.recordings.RecordingHelper.RecordingOptions;
import io.cryostat.recordings.RecordingHelper.RecordingReplace;
import io.cryostat.recordings.RecordingHelper.SnapshotCreationException;
import io.cryostat.targets.Target;
import io.cryostat.targets.TargetConnectionManager;
import io.cryostat.util.HttpMimeType;
import io.cryostat.ws.MessagingServer;
import io.cryostat.ws.Notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.annotation.security.RolesAllowed;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import jdk.jfr.RecordingState;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.multipart.FileUpload;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

@Path("")
public class Recordings {

    @Inject TargetConnectionManager connectionManager;
    @Inject EventBus bus;
    @Inject RecordingOptionsBuilderFactory recordingOptionsBuilderFactory;
    @Inject RecordingOptionsCustomizerFactory recordingOptionsCustomizerFactory;
    @Inject EventOptionsBuilder.Factory eventOptionsBuilderFactory;
    @Inject Clock clock;
    @Inject S3Client storage;
    @Inject StorageBuckets storageBuckets;
    @Inject S3Presigner presigner;
    @Inject RemoteRecordingInputStreamFactory remoteRecordingStreamFactory;
    @Inject ObjectMapper mapper;
    @Inject RecordingHelper recordingHelper;
    @Inject Logger logger;

    @Inject
    @Named(Producers.BASE64_URL)
    Base64 base64Url;

    @ConfigProperty(name = ConfigProperties.AWS_BUCKET_NAME_ARCHIVES)
    String bucket;

    @ConfigProperty(name = ConfigProperties.GRAFANA_DATASOURCE_URL)
    Optional<String> grafanaDatasourceURL;

    @ConfigProperty(name = ConfigProperties.STORAGE_TRANSIENT_ARCHIVES_ENABLED)
    boolean transientArchivesEnabled;

    @ConfigProperty(name = ConfigProperties.STORAGE_TRANSIENT_ARCHIVES_TTL)
    Duration transientArchivesTtl;

    @ConfigProperty(name = ConfigProperties.STORAGE_PRESIGNED_DOWNLOADS_ENABLED)
    boolean presignedDownloadsEnabled;

    @ConfigProperty(name = ConfigProperties.STORAGE_EXT_URL)
    Optional<String> externalStorageUrl;

    @ConfigProperty(name = ConfigProperties.CONNECTIONS_FAILED_TIMEOUT)
    Duration connectionFailedTimeout;

    void onStart(@Observes StartupEvent evt) {
        storageBuckets.createIfNecessary(bucket);
    }

    @GET
    @Blocking
    @Path("/api/v1/recordings")
    @RolesAllowed("read")
    public List<ArchivedRecording> listArchivesV1() {
        return recordingHelper.listArchivedRecordings();
    }

    @POST
    @Blocking
    @Path("/api/v1/recordings")
    @RolesAllowed("write")
    public Map<String, Object> upload(
            @RestForm("recording") FileUpload recording, @RestForm("labels") JsonObject rawLabels)
            throws Exception {
        Map<String, String> labels = new HashMap<>();
        if (rawLabels != null) {
            rawLabels.getMap().forEach((k, v) -> labels.put(k, v.toString()));
        }
        labels.put("jvmId", "uploads");
        labels.put("connectUrl", "uploads");
        Metadata metadata = new Metadata(labels);
        return doUpload(recording, metadata, "uploads");
    }

    @POST
    @Blocking
    @Path("/api/beta/recordings/{jvmId}")
    @RolesAllowed("write")
    public void agentPush(
            @RestPath String jvmId,
            @RestForm("recording") FileUpload recording,
            @RestForm("labels") JsonObject rawLabels,
            @RestForm("maxFiles") int maxFiles)
            throws Exception {
        jvmId = jvmId.strip();
        int max = Integer.MAX_VALUE;
        if (maxFiles > 0) {
            max = maxFiles;
        }
        Map<String, String> labels = new HashMap<>();
        if (rawLabels != null) {
            rawLabels.getMap().forEach((k, v) -> labels.put(k, v.toString()));
        }
        labels.put("jvmId", jvmId);
        Metadata metadata = new Metadata(labels);
        logger.tracev(
                "recording:{0}, labels:{1}, maxFiles:{2}", recording.fileName(), labels, maxFiles);
        doUpload(recording, metadata, jvmId);
        var objs = new ArrayList<S3Object>();
        recordingHelper.listArchivedRecordingObjects(jvmId).iterator().forEachRemaining(objs::add);
        var toRemove =
                objs.stream()
                        .sorted((a, b) -> b.lastModified().compareTo(a.lastModified()))
                        .skip(max)
                        .toList();
        if (toRemove.isEmpty()) {
            return;
        }
        logger.tracev("Removing {0}", toRemove);

        // FIXME this notification should be emitted in the deletion operation stream so that there
        // is one notification per deleted object
        var target = Target.getTargetByJvmId(jvmId);
        var event =
                new ArchivedRecordingEvent(
                        Recordings.RecordingEventCategory.ARCHIVED_DELETED,
                        ArchivedRecordingEvent.Payload.of(
                                target.map(t -> t.connectUrl).orElse(null),
                                new ArchivedRecording(
                                        jvmId,
                                        recording.fileName(),
                                        recordingHelper.downloadUrl(jvmId, recording.fileName()),
                                        recordingHelper.reportUrl(jvmId, recording.fileName()),
                                        metadata,
                                        0,
                                        clock.getMonotonicTime())));
        bus.publish(event.category().category(), event.payload().recording());
        bus.publish(
                MessagingServer.class.getName(),
                new Notification(event.category().category(), event.payload()));
        storage.deleteObjects(
                        DeleteObjectsRequest.builder()
                                .bucket(bucket)
                                .delete(
                                        Delete.builder()
                                                .objects(
                                                        toRemove.stream()
                                                                .map(S3Object::key)
                                                                .map(
                                                                        k ->
                                                                                ObjectIdentifier
                                                                                        .builder()
                                                                                        .key(k)
                                                                                        .build())
                                                                .toList())
                                                .build())
                                .build())
                .errors()
                .forEach(
                        err -> {
                            logger.errorv(
                                    "Deletion failure: {0} due to {1}", err.key(), err.message());
                        });
    }

    @GET
    @Blocking
    @Path("/api/beta/recordings/{jvmId}")
    @RolesAllowed("read")
    public List<ArchivedRecording> agentGet(@RestPath String jvmId) {
        var result = new ArrayList<ArchivedRecording>();
        recordingHelper
                .listArchivedRecordingObjects(jvmId)
                .forEach(
                        item -> {
                            String objectName = item.key().strip();
                            String filename = objectName.split("/")[1];
                            Metadata metadata =
                                    recordingHelper
                                            .getArchivedRecordingMetadata(jvmId, filename)
                                            .orElseGet(Metadata::empty);
                            result.add(
                                    new ArchivedRecording(
                                            jvmId,
                                            filename,
                                            recordingHelper.downloadUrl(jvmId, filename),
                                            recordingHelper.reportUrl(jvmId, filename),
                                            metadata,
                                            item.size(),
                                            item.lastModified().getEpochSecond()));
                        });
        return result;
    }

    @DELETE
    @Blocking
    @Path("/api/beta/recordings/{connectUrl}/{filename}")
    @RolesAllowed("write")
    public Response agentDelete(@RestPath String connectUrl, @RestPath String filename)
            throws Exception {
        String jvmId;
        if ("uploads".equals(connectUrl)) {
            jvmId = "uploads";
        } else {
            jvmId = Target.getTargetByConnectUrl(URI.create(connectUrl)).jvmId;
        }
        if (!recordingHelper.listArchivedRecordingObjects(jvmId).stream()
                .map(item -> item.key().strip().split("/")[1])
                .anyMatch(fn -> Objects.equals(fn, filename))) {
            return Response.status(RestResponse.Status.NOT_FOUND).build();
        }
        recordingHelper.deleteArchivedRecording(jvmId, filename);
        return Response.status(RestResponse.Status.NO_CONTENT).build();
    }

    @Blocking
    Map<String, Object> doUpload(FileUpload recording, Metadata metadata, String jvmId) {
        logger.tracev(
                "Upload: {0} {1} {2} {3}",
                recording.name(), recording.fileName(), recording.filePath(), metadata.labels);
        String filename = recording.fileName().strip();
        if (StringUtils.isBlank(filename)) {
            throw new BadRequestException();
        }
        if (!filename.endsWith(".jfr")) {
            filename = filename + ".jfr";
        }
        Map<String, String> labels = new HashMap<>(metadata.labels);
        labels.put("jvmId", jvmId);
        String key = recordingHelper.archivedRecordingKey(jvmId, filename);
        storage.putObject(
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType(RecordingHelper.JFR_MIME)
                        .tagging(recordingHelper.createMetadataTagging(new Metadata(labels)))
                        .build(),
                RequestBody.fromFile(recording.filePath()));
        logger.trace("Upload complete");

        var target = Target.getTargetByJvmId(jvmId);
        var event =
                new ArchivedRecordingEvent(
                        Recordings.RecordingEventCategory.ARCHIVED_CREATED,
                        ArchivedRecordingEvent.Payload.of(
                                target.map(t -> t.connectUrl).orElse(null),
                                new ArchivedRecording(
                                        jvmId,
                                        filename,
                                        recordingHelper.downloadUrl(jvmId, filename),
                                        recordingHelper.reportUrl(jvmId, filename),
                                        metadata,
                                        recording.size(),
                                        clock.getMonotonicTime())));
        bus.publish(event.category().category(), event.payload().recording());
        bus.publish(
                MessagingServer.class.getName(),
                new Notification(event.category().category(), event.payload()));

        return Map.of("name", filename, "metadata", Map.of("labels", metadata.labels));
    }

    @DELETE
    @Blocking
    @Path("/api/v1/recordings/{filename}")
    @RolesAllowed("write")
    public void delete(@RestPath String filename) throws Exception {
        // TODO scan all prefixes for matching filename? This is an old v1 API problem.
        storage.deleteObject(
                DeleteObjectRequest.builder()
                        .bucket(bucket)
                        .key(String.format("%s/%s", "uploads", filename))
                        .build());
    }

    @GET
    @Blocking
    @Path("/api/beta/fs/recordings")
    @RolesAllowed("read")
    public Collection<ArchivedRecordingDirectory> listFsArchives() {
        var map = new HashMap<String, ArchivedRecordingDirectory>();
        recordingHelper
                .listArchivedRecordingObjects()
                .forEach(
                        item -> {
                            String path = item.key().strip();
                            String[] parts = path.split("/");
                            String jvmId = parts[0];
                            String filename = parts[1];

                            Metadata metadata =
                                    recordingHelper
                                            .getArchivedRecordingMetadata(jvmId, filename)
                                            .orElseGet(Metadata::empty);

                            String connectUrl =
                                    metadata.labels.computeIfAbsent("connectUrl", k -> jvmId);
                            var dir =
                                    map.computeIfAbsent(
                                            jvmId,
                                            id ->
                                                    new ArchivedRecordingDirectory(
                                                            connectUrl, id, new ArrayList<>()));
                            dir.recordings.add(
                                    new ArchivedRecording(
                                            jvmId,
                                            filename,
                                            recordingHelper.downloadUrl(jvmId, filename),
                                            recordingHelper.reportUrl(jvmId, filename),
                                            metadata,
                                            item.size(),
                                            item.lastModified().getEpochSecond()));
                        });
        return map.values();
    }

    @GET
    @Blocking
    @Path("/api/beta/fs/recordings/{jvmId}")
    @RolesAllowed("read")
    public Collection<ArchivedRecordingDirectory> listFsArchives(@RestPath String jvmId) {
        var map = new HashMap<String, ArchivedRecordingDirectory>();
        recordingHelper
                .listArchivedRecordingObjects(jvmId)
                .forEach(
                        item -> {
                            String filename = item.key().strip().replace(jvmId + "/", "");

                            Metadata metadata =
                                    recordingHelper
                                            .getArchivedRecordingMetadata(jvmId, filename)
                                            .orElseGet(Metadata::empty);

                            String connectUrl =
                                    metadata.labels.computeIfAbsent("connectUrl", k -> jvmId);
                            var dir =
                                    map.computeIfAbsent(
                                            jvmId,
                                            id ->
                                                    new ArchivedRecordingDirectory(
                                                            connectUrl, id, new ArrayList<>()));
                            dir.recordings.add(
                                    new ArchivedRecording(
                                            jvmId,
                                            filename,
                                            recordingHelper.downloadUrl(jvmId, filename),
                                            recordingHelper.reportUrl(jvmId, filename),
                                            metadata,
                                            item.size(),
                                            item.lastModified().getEpochSecond()));
                        });
        return map.values();
    }

    @GET
    @Transactional
    @Path("/api/v3/targets/{id}/recordings")
    @RolesAllowed("read")
    public List<LinkedRecordingDescriptor> listForTarget(@RestPath long id) throws Exception {
        Target target = Target.find("id", id).singleResult();
        return recordingHelper.listActiveRecordings(target).stream()
                .map(recordingHelper::toExternalForm)
                .toList();
    }

    @GET
    @Path("/api/v1/targets/{connectUrl}/recordings")
    @RolesAllowed("read")
    public Response listForTargetByUrl(@RestPath URI connectUrl) throws Exception {
        Target target = Target.getTargetByConnectUrl(connectUrl);
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(URI.create(String.format("/api/v3/targets/%d/recordings", target.id)))
                .build();
    }

    @PATCH
    @Transactional
    @Blocking
    @Path("/api/v3/targets/{targetId}/recordings/{remoteId}")
    @RolesAllowed("write")
    public String patch(@RestPath long targetId, @RestPath long remoteId, String body)
            throws Exception {
        Target target = Target.find("id", targetId).singleResult();
        Optional<ActiveRecording> recording =
                recordingHelper.listActiveRecordings(target).stream()
                        .filter(rec -> rec.remoteId == remoteId)
                        .findFirst();
        if (!recording.isPresent()) {
            throw new NotFoundException();
        }
        ActiveRecording activeRecording = recording.get();
        switch (body.toLowerCase()) {
            case "stop":
                recordingHelper
                        .stopRecording(activeRecording)
                        .await()
                        .atMost(connectionFailedTimeout);
                return null;
            case "save":
                try {
                    // FIXME this operation might take a long time to complete, depending on the
                    // amount of JFR data in the target and the speed of the connection between the
                    // target and Cryostat. We should not make the client wait until this operation
                    // completes before sending a response - it should be async. Here we should just
                    // return an Accepted response, and if a failure occurs that should be indicated
                    // as a websocket notification.
                    return recordingHelper.archiveRecording(activeRecording, null, null).name();
                } catch (IOException ioe) {
                    logger.warn(ioe);
                    return null;
                }
            default:
                throw new BadRequestException(body);
        }
    }

    @PATCH
    @Transactional
    @Blocking
    @Path("/api/v1/targets/{connectUrl}/recordings/{recordingName}")
    @RolesAllowed("write")
    public Response patchV1(@RestPath URI connectUrl, @RestPath String recordingName, String body)
            throws Exception {
        Target target = Target.getTargetByConnectUrl(connectUrl);
        Optional<IRecordingDescriptor> recording =
                connectionManager.executeConnectedTask(
                        target, conn -> recordingHelper.getDescriptorByName(conn, recordingName));
        if (recording.isEmpty()) {
            throw new NotFoundException();
        }
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(
                        URI.create(
                                String.format(
                                        "/api/v3/targets/%d/recordings/%s",
                                        target.id, recording.get().getId())))
                .build();
    }

    @POST
    @Transactional
    @Path("/api/v1/targets/{connectUrl}/snapshot")
    @RolesAllowed("write")
    public Uni<Response> createSnapshotV1(@RestPath URI connectUrl) throws Exception {
        Target target = Target.getTargetByConnectUrl(connectUrl);
        return recordingHelper
                .createSnapshot(target)
                .onItem()
                .transform(
                        recording ->
                                Response.status(Response.Status.OK).entity(recording.name).build())
                .onFailure(SnapshotCreationException.class)
                .recoverWithItem(Response.status(Response.Status.ACCEPTED).build());
    }

    @POST
    @Transactional
    @Path("/api/v2/targets/{connectUrl}/snapshot")
    @RolesAllowed("write")
    public Uni<Response> createSnapshotV2(@RestPath URI connectUrl) throws Exception {
        Target target = Target.getTargetByConnectUrl(connectUrl);
        return recordingHelper
                .createSnapshot(target)
                .onItem()
                .transform(
                        recording ->
                                Response.status(Response.Status.CREATED)
                                        .entity(
                                                V2Response.json(
                                                        Response.Status.CREATED,
                                                        recordingHelper.toExternalForm(recording)))
                                        .build())
                .onFailure(SnapshotCreationException.class)
                .recoverWithItem(
                        Response.status(Response.Status.ACCEPTED)
                                .entity(V2Response.json(Response.Status.ACCEPTED, null))
                                .build());
    }

    @POST
    @Transactional
    @Path("/api/v3/targets/{id}/snapshot")
    @RolesAllowed("write")
    public Uni<Response> createSnapshot(@RestPath long id) throws Exception {
        Target target = Target.find("id", id).singleResult();
        return recordingHelper
                .createSnapshot(target)
                .onItem()
                .transform(
                        recording ->
                                Response.status(Response.Status.OK)
                                        .entity(recordingHelper.toExternalForm(recording))
                                        .build())
                .onFailure(SnapshotCreationException.class)
                .recoverWithItem(Response.status(Response.Status.ACCEPTED).build());
    }

    @POST
    @Transactional
    @Blocking
    @Path("/api/v3/targets/{id}/recordings")
    @RolesAllowed("write")
    public Response createRecording(
            @RestPath long id,
            @RestForm String recordingName,
            @RestForm String events,
            @RestForm Optional<String> replace,
            // restart param is deprecated, only 'replace' should be used and takes priority if both
            // are provided
            @Deprecated @RestForm Optional<Boolean> restart,
            @RestForm Optional<Long> duration,
            @RestForm Optional<Boolean> toDisk,
            @RestForm Optional<Long> maxAge,
            @RestForm Optional<Long> maxSize,
            @RestForm("metadata") Optional<String> rawMetadata,
            @RestForm Optional<Boolean> archiveOnStop)
            throws Exception {
        if (StringUtils.isBlank(recordingName)) {
            throw new BadRequestException("\"recordingName\" form parameter must be provided");
        }
        if (StringUtils.isBlank(events)) {
            throw new BadRequestException("\"events\" form parameter must be provided");
        }

        Target target = Target.find("id", id).singleResult();

        Pair<String, TemplateType> pair = recordingHelper.parseEventSpecifier(events);
        Template template =
                recordingHelper.getPreferredTemplate(target, pair.getKey(), pair.getValue());

        Map<String, String> labels = new HashMap<>();
        if (rawMetadata.isPresent()) {
            labels.putAll(mapper.readValue(rawMetadata.get(), Metadata.class).labels);
        }
        RecordingReplace replacement = RecordingReplace.NEVER;
        if (replace.isPresent()) {
            replacement = RecordingReplace.fromString(replace.get());
        } else if (restart.isPresent()) {
            replacement = restart.get() ? RecordingReplace.ALWAYS : RecordingReplace.NEVER;
        }
        ActiveRecording recording =
                recordingHelper
                        .startRecording(
                                target,
                                replacement,
                                template,
                                new RecordingOptions(
                                        recordingName,
                                        toDisk,
                                        archiveOnStop,
                                        duration,
                                        maxSize,
                                        maxAge),
                                labels)
                        .await()
                        .atMost(Duration.ofSeconds(10));

        return Response.status(Response.Status.CREATED)
                .entity(recordingHelper.toExternalForm(recording))
                .build();
    }

    @POST
    @Transactional
    @Blocking
    @Path("/api/v1/targets/{connectUrl}/recordings")
    @RolesAllowed("write")
    public Response createRecordingV1(@RestPath URI connectUrl) throws Exception {
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(
                        URI.create(
                                String.format(
                                        "/api/v3/targets/%d/recordings",
                                        Target.getTargetByConnectUrl(connectUrl).id)))
                .build();
    }

    @DELETE
    @Transactional
    @Blocking
    @Path("/api/v1/targets/{connectUrl}/recordings/{recordingName}")
    @RolesAllowed("write")
    public Response deleteRecordingV1(@RestPath URI connectUrl, @RestPath String recordingName)
            throws Exception {
        if (StringUtils.isBlank(recordingName)) {
            throw new BadRequestException("\"recordingName\" form parameter must be provided");
        }
        Target target = Target.getTargetByConnectUrl(connectUrl);
        long remoteId =
                recordingHelper.listActiveRecordings(target).stream()
                        .filter(r -> Objects.equals(r.name, recordingName))
                        .findFirst()
                        .map(r -> r.remoteId)
                        .orElseThrow(() -> new NotFoundException());
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(
                        URI.create(
                                String.format(
                                        "/api/v3/targets/%d/recordings/%d", target.id, remoteId)))
                .build();
    }

    @DELETE
    @Transactional
    @Blocking
    @Path("/api/v3/targets/{targetId}/recordings/{remoteId}")
    @RolesAllowed("write")
    public void deleteRecording(@RestPath long targetId, @RestPath long remoteId) throws Exception {
        Target target = Target.find("id", targetId).singleResult();
        var recording = target.getRecordingById(remoteId);
        if (recording == null) {
            throw new NotFoundException();
        }
        recordingHelper.deleteRecording(recording).await().atMost(connectionFailedTimeout);
    }

    @DELETE
    @Blocking
    @Path("/api/beta/fs/recordings/{jvmId}/{filename}")
    @RolesAllowed("write")
    public void deleteArchivedRecording(@RestPath String jvmId, @RestPath String filename)
            throws Exception {
        logger.tracev("Handling archived recording deletion: {0} / {1}", jvmId, filename);
        var metadata =
                recordingHelper
                        .getArchivedRecordingMetadata(jvmId, filename)
                        .orElseGet(Metadata::empty);

        var connectUrl =
                Target.getTargetByJvmId(jvmId)
                        .map(t -> t.connectUrl)
                        .map(c -> c.toString())
                        .filter(StringUtils::isNotBlank)
                        .orElseGet(
                                () ->
                                        metadata.labels.computeIfAbsent(
                                                "connectUrl", k -> "lost-" + jvmId));
        logger.tracev(
                "Archived recording from connectUrl \"{0}\" has metadata: {1}",
                connectUrl, metadata);
        logger.tracev(
                "Sending S3 deletion request for {0} {1}",
                bucket, recordingHelper.archivedRecordingKey(jvmId, filename));
        var resp =
                storage.deleteObject(
                        DeleteObjectRequest.builder()
                                .bucket(bucket)
                                .key(recordingHelper.archivedRecordingKey(jvmId, filename))
                                .build());
        logger.tracev(
                "Got SDK response {0} {1}",
                resp.sdkHttpResponse().statusCode(), resp.sdkHttpResponse().statusText());
        if (resp.sdkHttpResponse().isSuccessful()) {
            var event =
                    new ArchivedRecordingEvent(
                            Recordings.RecordingEventCategory.ARCHIVED_DELETED,
                            ArchivedRecordingEvent.Payload.of(
                                    URI.create(connectUrl),
                                    new ArchivedRecording(
                                            jvmId,
                                            filename,
                                            recordingHelper.downloadUrl(jvmId, filename),
                                            recordingHelper.reportUrl(jvmId, filename),
                                            metadata,
                                            0 /*filesize*/,
                                            clock.getMonotonicTime())));
            bus.publish(event.category().category(), event.payload().recording());
            bus.publish(
                    MessagingServer.class.getName(),
                    new Notification(event.category().category(), event.payload()));
        } else {
            throw new HttpException(
                    resp.sdkHttpResponse().statusCode(),
                    resp.sdkHttpResponse().statusText().orElse(""));
        }
    }

    @POST
    @Blocking
    @Transactional
    @Path("/api/v1/targets/{connectUrl}/recordings/{recordingName}/upload")
    @RolesAllowed("write")
    public Response uploadActiveToGrafanaV1(
            @RestPath URI connectUrl, @RestPath String recordingName) {
        Target target = Target.getTargetByConnectUrl(connectUrl);
        long remoteId =
                recordingHelper.listActiveRecordings(target).stream()
                        .filter(r -> Objects.equals(r.name, recordingName))
                        .findFirst()
                        .map(r -> r.remoteId)
                        .orElseThrow(() -> new NotFoundException());
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(
                        URI.create(
                                String.format(
                                        "/api/v3/targets/%d/recordings/%d/upload",
                                        target.id, remoteId)))
                .build();
    }

    @POST
    @Blocking
    @Path("/api/v3/targets/{targetId}/recordings/{remoteId}/upload")
    @RolesAllowed("write")
    public Uni<String> uploadActiveToGrafana(@RestPath long targetId, @RestPath long remoteId)
            throws Exception {
        return recordingHelper.uploadToJFRDatasource(targetId, remoteId);
    }

    @POST
    @Path("/api/beta/recordings/{connectUrl}/{filename}/upload")
    @RolesAllowed("write")
    public Response uploadArchivedToGrafanaBeta(
            @RestPath String connectUrl, @RestPath String filename) throws Exception {
        String jvmId;
        if ("uploads".equals(connectUrl)) {
            jvmId = "uploads";
        } else {
            jvmId = Target.getTargetByConnectUrl(URI.create(connectUrl)).jvmId;
        }
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(
                        URI.create(
                                String.format(
                                        "/api/v3/grafana/%s",
                                        recordingHelper.encodedKey(jvmId, filename))))
                .build();
    }

    @POST
    @Path("/api/beta/fs/recordings/{jvmId}/{filename}/upload")
    @RolesAllowed("write")
    public Response uploadArchivedToGrafanaFromPath(
            @RestPath String jvmId, @RestPath String filename) throws Exception {
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(
                        URI.create(
                                String.format(
                                        "/api/v3/grafana/%s",
                                        recordingHelper.encodedKey(jvmId, filename))))
                .build();
    }

    @POST
    @Blocking
    @Path("/api/v3/grafana/{encodedKey}")
    @RolesAllowed("write")
    public Uni<String> uploadArchivedToGrafana(@RestPath String encodedKey) throws Exception {
        var key = recordingHelper.decodedKey(encodedKey);
        var found =
                recordingHelper.listArchivedRecordingObjects().stream()
                        .anyMatch(
                                o ->
                                        Objects.equals(
                                                o.key(),
                                                recordingHelper.archivedRecordingKey(key)));
        if (!found) {
            throw new NotFoundException();
        }
        return recordingHelper.uploadToJFRDatasource(key);
    }

    @GET
    @Blocking
    @Path("/api/v1/targets/{connectUrl}/recordingOptions")
    @RolesAllowed("read")
    public Response getRecordingOptionsV1(@RestPath URI connectUrl) throws Exception {
        Target target = Target.getTargetByConnectUrl(connectUrl);
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(
                        URI.create(String.format("/api/v3/targets/%d/recordingOptions", target.id)))
                .build();
    }

    @GET
    @Blocking
    @Path("/api/v3/targets/{id}/recordingOptions")
    @RolesAllowed("read")
    public Map<String, Object> getRecordingOptions(@RestPath long id) throws Exception {
        Target target = Target.find("id", id).singleResult();
        return connectionManager.executeConnectedTask(
                target,
                connection -> {
                    RecordingOptionsBuilder builder = recordingOptionsBuilderFactory.create(target);
                    return getRecordingOptions(connection.getService(), builder);
                });
    }

    @PATCH
    @Blocking
    @Path("/api/v1/targets/{connectUrl}/recordingOptions")
    @RolesAllowed("write")
    public Response patchRecordingOptionsV1(@RestPath URI connectUrl) {
        Target target = Target.getTargetByConnectUrl(connectUrl);
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(
                        URI.create(String.format("/api/v3/targets/%d/recordingOptions", target.id)))
                .build();
    }

    @PATCH
    @Blocking
    @Path("/api/v3/targets/{id}/recordingOptions")
    @RolesAllowed("read")
    @SuppressFBWarnings(
            value = "UC_USELESS_OBJECT",
            justification = "SpotBugs thinks the options map is unused, but it is used")
    public Map<String, Object> patchRecordingOptions(
            @RestPath long id,
            @RestForm String toDisk,
            @RestForm String maxAge,
            @RestForm String maxSize)
            throws Exception {
        final String unsetKeyword = "unset";

        Map<String, String> options = new HashMap<>();
        Pattern bool = Pattern.compile("true|false|" + unsetKeyword);
        if (toDisk != null) {
            Matcher m = bool.matcher(toDisk);
            if (!m.matches()) {
                throw new BadRequestException("Invalid options");
            }
            options.put("toDisk", toDisk);
        }
        if (maxAge != null) {
            if (!unsetKeyword.equals(maxAge)) {
                try {
                    Long.parseLong(maxAge);
                    options.put("maxAge", maxAge);
                } catch (NumberFormatException e) {
                    throw new BadRequestException("Invalid options");
                }
            }
        }
        if (maxSize != null) {
            if (!unsetKeyword.equals(maxSize)) {
                try {
                    Long.parseLong(maxSize);
                    options.put("maxSize", maxSize);
                } catch (NumberFormatException e) {
                    throw new BadRequestException("Invalid options");
                }
            }
        }
        Target target = Target.find("id", id).singleResult();
        for (var entry : options.entrySet()) {
            RecordingOptionsCustomizer.OptionKey optionKey =
                    RecordingOptionsCustomizer.OptionKey.fromOptionName(entry.getKey()).get();
            var recordingOptionsCustomizer = recordingOptionsCustomizerFactory.create(target);
            if (unsetKeyword.equals(entry.getValue())) {
                recordingOptionsCustomizer.unset(optionKey);
            } else {
                recordingOptionsCustomizer.set(optionKey, entry.getValue());
            }
        }

        return connectionManager.executeConnectedTask(
                target,
                connection -> {
                    var builder = recordingOptionsBuilderFactory.create(target);
                    return getRecordingOptions(connection.getService(), builder);
                });
    }

    @GET
    @Blocking
    @Path("/api/v3/activedownload/{id}")
    @RolesAllowed("read")
    public Response handleActiveDownload(@RestPath long id) throws Exception {
        ActiveRecording recording = ActiveRecording.find("id", id).singleResult();
        if (!transientArchivesEnabled) {
            return Response.status(RestResponse.Status.OK)
                    .header(
                            HttpHeaders.CONTENT_DISPOSITION,
                            String.format("attachment; filename=\"%s.jfr\"", recording.name))
                    .header(HttpHeaders.CONTENT_TYPE, HttpMimeType.OCTET_STREAM.mime())
                    .entity(recordingHelper.getActiveInputStream(recording))
                    .build();
        }

        String savename = recording.name;
        String filename =
                recordingHelper
                        .archiveRecording(
                                recording, savename, Instant.now().plus(transientArchivesTtl))
                        .name();
        String encodedKey = recordingHelper.encodedKey(recording.target.jvmId, filename);
        if (!savename.endsWith(".jfr")) {
            savename += ".jfr";
        }
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .header(
                        HttpHeaders.CONTENT_DISPOSITION,
                        String.format("attachment; filename=\"%s\"", savename))
                .location(
                        URI.create(
                                String.format(
                                        "/api/v3/download/%s?f=%s",
                                        encodedKey,
                                        base64Url.encodeAsString(
                                                savename.getBytes(StandardCharsets.UTF_8)))))
                .build();
    }

    @GET
    @Blocking
    @Path("/api/v3/download/{encodedKey}")
    @RolesAllowed("read")
    public Response handleStorageDownload(@RestPath String encodedKey, @RestQuery String f)
            throws URISyntaxException {
        Pair<String, String> pair = recordingHelper.decodedKey(encodedKey);

        if (!presignedDownloadsEnabled) {
            return Response.status(RestResponse.Status.OK)
                    .header(
                            HttpHeaders.CONTENT_DISPOSITION,
                            String.format("attachment; filename=\"%s\"", pair.getValue()))
                    .header(HttpHeaders.CONTENT_TYPE, HttpMimeType.OCTET_STREAM.mime())
                    .entity(recordingHelper.getArchivedRecordingStream(encodedKey))
                    .build();
        }

        logger.tracev("Handling presigned download request for {0}", pair);
        GetObjectRequest getRequest =
                GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(recordingHelper.archivedRecordingKey(pair))
                        .build();
        GetObjectPresignRequest presignRequest =
                GetObjectPresignRequest.builder()
                        .signatureDuration(Duration.ofMinutes(1))
                        .getObjectRequest(getRequest)
                        .build();
        PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(presignRequest);
        URI uri = presignedRequest.url().toURI();
        if (externalStorageUrl.isPresent()) {
            String extUrl = externalStorageUrl.get();
            if (StringUtils.isNotBlank(extUrl)) {
                URI extUri = new URI(extUrl);
                uri =
                        new URI(
                                extUri.getScheme(),
                                extUri.getAuthority(),
                                URI.create(String.format("%s/%s", extUri.getPath(), uri.getPath()))
                                        .normalize()
                                        .getPath(),
                                uri.getQuery(),
                                uri.getFragment());
            }
        }
        ResponseBuilder response = Response.status(RestResponse.Status.PERMANENT_REDIRECT);
        if (StringUtils.isNotBlank(f)) {
            response =
                    response.header(
                            HttpHeaders.CONTENT_DISPOSITION,
                            String.format(
                                    "attachment; filename=\"%s\"",
                                    new String(base64Url.decode(f), StandardCharsets.UTF_8)));
        }
        return response.location(uri).build();
    }

    private static Map<String, Object> getRecordingOptions(
            IFlightRecorderService service, RecordingOptionsBuilder builder) throws Exception {
        IConstrainedMap<String> recordingOptions = builder.build();

        Map<String, IOptionDescriptor<?>> targetRecordingOptions =
                service.getAvailableRecordingOptions();

        Map<String, Object> map = new HashMap<String, Object>();

        if (recordingOptions.get("toDisk") != null) {
            map.put("toDisk", recordingOptions.get("toDisk"));
        } else {
            map.put("toDisk", targetRecordingOptions.get("disk").getDefault());
        }

        map.put("maxAge", getNumericOption("maxAge", recordingOptions, targetRecordingOptions));
        map.put("maxSize", getNumericOption("maxSize", recordingOptions, targetRecordingOptions));

        return map;
    }

    private static Long getNumericOption(
            String name,
            IConstrainedMap<String> defaultOptions,
            Map<String, IOptionDescriptor<?>> targetOptions) {
        Object value;

        if (defaultOptions.get(name) != null) {
            value = defaultOptions.get(name);
        } else {
            value = targetOptions.get(name).getDefault();
        }

        if (value instanceof Number) {
            return Long.valueOf(((Number) value).longValue());
        }
        return null;
    }

    public record LinkedRecordingDescriptor(
            long id,
            long remoteId,
            RecordingState state,
            long duration,
            long startTime,
            boolean continuous,
            boolean toDisk,
            long maxSize,
            long maxAge,
            String name,
            String downloadUrl,
            String reportUrl,
            Metadata metadata) {
        public LinkedRecordingDescriptor {
            Objects.requireNonNull(state);
            Objects.requireNonNull(name);
            Objects.requireNonNull(downloadUrl);
            Objects.requireNonNull(reportUrl);
            Objects.requireNonNull(metadata);
        }
    }

    // TODO include jvmId and filename
    public record ArchivedRecording(
            String jvmId,
            String name,
            String downloadUrl,
            String reportUrl,
            Metadata metadata,
            long size,
            long archivedTime) {
        public ArchivedRecording {
            Objects.requireNonNull(jvmId);
            Objects.requireNonNull(name);
            Objects.requireNonNull(downloadUrl);
            Objects.requireNonNull(reportUrl);
            Objects.requireNonNull(metadata);
        }
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public record ArchivedRecordingDirectory(
            String connectUrl, String jvmId, List<ArchivedRecording> recordings) {
        public ArchivedRecordingDirectory {
            Objects.requireNonNull(connectUrl);
            Objects.requireNonNull(jvmId);
            if (recordings == null) {
                recordings = Collections.emptyList();
            }
        }
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public record Metadata(Map<String, String> labels, Instant expiry) {
        public Metadata {
            Objects.requireNonNull(labels);
        }

        public Metadata(Map<String, String> labels) {
            this(labels, null);
        }

        public Metadata(Metadata other) {
            this(new HashMap<>((other.labels)));
        }

        public Metadata(Metadata other, Instant expiry) {
            this(new HashMap<>((other.labels)), expiry);
        }

        public static Metadata empty() {
            return new Metadata(new HashMap<>());
        }
    }

    public static final String ACTIVE_RECORDING_CREATED = "ActiveRecordingCreated";
    public static final String ACTIVE_RECORDING_STOPPED = "ActiveRecordingStopped";
    public static final String ARCHIVED_RECORDING_DELETED = "ArchivedRecordingDeleted";
    public static final String ARCHIVED_RECORDING_CREATED = "ArchivedRecordingCreated";
    public static final String ACTIVE_RECORDING_DELETED = "ActiveRecordingDeleted";
    public static final String ACTIVE_RECORDING_SAVED = "ActiveRecordingSaved";
    public static final String SNAPSHOT_RECORDING_CREATED = "SnapshotCreated";
    public static final String RECORDING_METADATA_UPDATED = "RecordingMetadataUpdated";

    public enum RecordingEventCategory {
        ACTIVE_CREATED(ACTIVE_RECORDING_CREATED),
        ACTIVE_STOPPED(ACTIVE_RECORDING_STOPPED),
        ACTIVE_SAVED(ACTIVE_RECORDING_SAVED),
        ACTIVE_DELETED(ACTIVE_RECORDING_DELETED),
        ARCHIVED_CREATED(ARCHIVED_RECORDING_CREATED),
        ARCHIVED_DELETED(ARCHIVED_RECORDING_DELETED),
        SNAPSHOT_CREATED(SNAPSHOT_RECORDING_CREATED),
        METADATA_UPDATED(RECORDING_METADATA_UPDATED),
        ;

        private final String category;

        private RecordingEventCategory(String category) {
            this.category = category;
        }

        public String category() {
            return category;
        }
    }
}
