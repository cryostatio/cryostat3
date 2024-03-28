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
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openjdk.jmc.common.unit.IConstrainedMap;
import org.openjdk.jmc.flightrecorder.configuration.recording.RecordingOptionsBuilder;
import org.openjdk.jmc.rjmx.services.jfr.IRecordingDescriptor;

import io.cryostat.ConfigProperties;
import io.cryostat.Producers;
import io.cryostat.core.EventOptionsBuilder;
import io.cryostat.core.FlightRecorderException;
import io.cryostat.core.net.JFRConnection;
import io.cryostat.core.sys.Clock;
import io.cryostat.core.sys.FileSystem;
import io.cryostat.core.templates.Template;
import io.cryostat.core.templates.TemplateType;
import io.cryostat.events.EventTemplates;
import io.cryostat.events.S3TemplateService;
import io.cryostat.events.TargetTemplateService;
import io.cryostat.recordings.ActiveRecording.Listener.ActiveRecordingEvent;
import io.cryostat.recordings.ActiveRecording.Listener.ArchivedRecordingEvent;
import io.cryostat.recordings.Recordings.ArchivedRecording;
import io.cryostat.recordings.Recordings.LinkedRecordingDescriptor;
import io.cryostat.recordings.Recordings.Metadata;
import io.cryostat.targets.Target;
import io.cryostat.targets.TargetConnectionManager;
import io.cryostat.util.EntityExistsException;
import io.cryostat.util.HttpMimeType;
import io.cryostat.ws.MessagingServer;
import io.cryostat.ws.Notification;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.mutiny.core.eventbus.EventBus;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.ext.web.multipart.MultipartForm;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.ServerErrorException;
import jdk.jfr.RecordingState;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.hc.core5.http.HttpStatus;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

@ApplicationScoped
public class RecordingHelper {

    public static final String JFR_MIME = HttpMimeType.JFR.mime();

    private static final Pattern TEMPLATE_PATTERN =
            Pattern.compile("^template=([\\w]+)(?:,type=([\\w]+))?$");
    public static final String DATASOURCE_FILENAME = "cryostat-analysis.jfr";

    @Inject S3Client storage;

    @Inject WebClient webClient;
    @Inject FileSystem fs;
    @Inject Clock clock;
    @Inject TargetConnectionManager connectionManager;
    @Inject RemoteRecordingInputStreamFactory remoteRecordingStreamFactory;
    @Inject RecordingOptionsBuilderFactory recordingOptionsBuilderFactory;
    @Inject EventOptionsBuilder.Factory eventOptionsBuilderFactory;
    @Inject TargetTemplateService.Factory targetTemplateServiceFactory;
    @Inject S3TemplateService customTemplateService;
    @Inject Scheduler scheduler;
    private final List<JobKey> jobs = new CopyOnWriteArrayList<>();

    @Inject
    @Named(Producers.BASE64_URL)
    Base64 base64Url;

    @Inject EventBus bus;
    @Inject Logger logger;

    @ConfigProperty(name = ConfigProperties.AWS_BUCKET_NAME_ARCHIVES)
    String archiveBucket;

    @ConfigProperty(name = ConfigProperties.AWS_OBJECT_EXPIRATION_LABELS)
    String objectExpirationLabel;

    @ConfigProperty(name = ConfigProperties.CONNECTIONS_FAILED_TIMEOUT)
    Duration connectionFailedTimeout;

    @ConfigProperty(name = ConfigProperties.GRAFANA_DATASOURCE_URL)
    Optional<String> grafanaDatasourceURLProperty;

    CompletableFuture<URL> grafanaDatasourceURL = new CompletableFuture<>();

    void onStart(@Observes StartupEvent evt) {
        if (grafanaDatasourceURLProperty.isEmpty()) {
            grafanaDatasourceURL.completeExceptionally(
                    new HttpException(
                            HttpStatus.SC_BAD_GATEWAY,
                            String.format(
                                    "Configuration property %s is not set",
                                    ConfigProperties.GRAFANA_DATASOURCE_URL)));
            return;
        }
        try {
            URL uploadUrl =
                    new URL(grafanaDatasourceURLProperty.orElseThrow(() -> new HttpException()));
            boolean isValidUploadUrl =
                    new UrlValidator(UrlValidator.ALLOW_LOCAL_URLS).isValid(uploadUrl.toString());
            if (!isValidUploadUrl) {
                grafanaDatasourceURL.completeExceptionally(
                        new HttpException(
                                HttpStatus.SC_BAD_GATEWAY,
                                String.format(
                                        "Configuration property %s=%s is not acceptable",
                                        ConfigProperties.GRAFANA_DATASOURCE_URL,
                                        grafanaDatasourceURLProperty.get())));
                return;
            }
            grafanaDatasourceURL.complete(new URL(grafanaDatasourceURLProperty.get()));
        } catch (MalformedURLException e) {
            grafanaDatasourceURL.completeExceptionally(
                    new HttpException(
                            HttpStatus.SC_BAD_GATEWAY,
                            String.format(
                                    "Configuration property %s=%s is not a valid URL",
                                    ConfigProperties.GRAFANA_DATASOURCE_URL,
                                    grafanaDatasourceURLProperty.get())));
            return;
        }
    }

    @Blocking
    public void updateRemoteRecordings(Target target) {
        try {
            target.activeRecordings.clear();
            target.persist();
            List<IRecordingDescriptor> descriptors =
                    connectionManager.executeConnectedTask(
                            target, conn -> conn.getService().getAvailableRecordings());
            for (var descriptor : descriptors) {
                // TODO is there any metadata to attach here?
                var recording = ActiveRecording.from(target, descriptor, new Metadata(Map.of()));
                recording.persist();
                target.activeRecordings.add(recording);
            }
            target.persist();
        } catch (Exception e) {
            logger.errorv(
                    e,
                    "Failure to synchronize existing target recording state for {0}",
                    target.connectUrl);
        }
    }

    @Blocking
    public ActiveRecording startRecording(
            Target target,
            IConstrainedMap<String> recordingOptions,
            Template eventTemplate,
            Metadata metadata,
            boolean archiveOnStop,
            RecordingReplace replace,
            JFRConnection connection)
            throws Exception {
        String recordingName = (String) recordingOptions.get(RecordingOptionsBuilder.KEY_NAME);
        getDescriptorByName(connection, recordingName)
                .ifPresent(
                        previous -> {
                            RecordingState previousState = mapState(previous);
                            boolean restart =
                                    shouldRestartRecording(replace, previousState, recordingName);
                            if (!restart) {
                                throw new EntityExistsException("Recording", recordingName);
                            }
                            target.activeRecordings.stream()
                                    .filter(r -> r.name.equals(recordingName))
                                    .forEach(this::deleteRecording);
                        });

        IRecordingDescriptor desc =
                connection
                        .getService()
                        .start(recordingOptions, eventTemplate.getName(), eventTemplate.getType());

        Map<String, String> labels = metadata.labels();
        labels.put("template.name", eventTemplate.getName());
        labels.put("template.type", eventTemplate.getType().toString());
        Metadata meta = new Metadata(labels);

        ActiveRecording recording = ActiveRecording.from(target, desc, meta);
        recording.persist();

        target.activeRecordings.add(recording);
        target.persist();

        if (!recording.continuous) {
            JobDetail jobDetail =
                    JobBuilder.newJob(StopRecordingJob.class)
                            .withIdentity(recording.name, target.jvmId)
                            .build();
            if (!jobs.contains(jobDetail.getKey())) {
                Map<String, Object> data = jobDetail.getJobDataMap();
                data.put("recording", recording);
                data.put("archive", archiveOnStop);
                Trigger trigger =
                        TriggerBuilder.newTrigger()
                                .withIdentity(recording.name, target.jvmId)
                                .usingJobData(jobDetail.getJobDataMap())
                                .startAt(new Date(System.currentTimeMillis() + recording.duration))
                                .build();
                scheduler.scheduleJob(jobDetail, trigger);
            }
        }

        return recording;
    }

    @Blocking
    public ActiveRecording createSnapshot(Target target, JFRConnection connection)
            throws Exception {
        IRecordingDescriptor desc = connection.getService().getSnapshotRecording();

        String rename = String.format("%s-%d", desc.getName().toLowerCase(), desc.getId());

        RecordingOptionsBuilder recordingOptionsBuilder =
                recordingOptionsBuilderFactory.create(connection.getService());
        recordingOptionsBuilder.name(rename);

        connection.getService().updateRecordingOptions(desc, recordingOptionsBuilder.build());

        Optional<IRecordingDescriptor> updatedDescriptor = getDescriptorByName(connection, rename);

        if (updatedDescriptor.isEmpty()) {
            throw new IllegalStateException(
                    "The most recent snapshot of the recording cannot be"
                            + " found after renaming.");
        }

        desc = updatedDescriptor.get();

        try (InputStream snapshot = remoteRecordingStreamFactory.open(connection, target, desc)) {
            if (!snapshotIsReadable(target, snapshot)) {
                connection.getService().close(desc);
                throw new SnapshotCreationException(
                        "Snapshot was not readable - are there any source recordings?");
            }
        }

        ActiveRecording recording =
                ActiveRecording.from(
                        target,
                        desc,
                        new Metadata(
                                Map.of(
                                        "jvmId",
                                        target.jvmId,
                                        "connectUrl",
                                        target.connectUrl.toString())));
        recording.persist();

        target.activeRecordings.add(recording);
        target.persist();

        var event =
                new ActiveRecordingEvent(
                        Recordings.RecordingEventCategory.SNAPSHOT_CREATED,
                        ActiveRecordingEvent.Payload.of(this, recording));
        bus.publish(event.category().category(), event.payload().recording());
        bus.publish(
                MessagingServer.class.getName(),
                new Notification(event.category().category(), event.payload()));

        return recording;
    }

    @Blocking
    private boolean snapshotIsReadable(Target target, InputStream snapshot) throws IOException {
        if (!connectionManager.markConnectionInUse(target)) {
            throw new IOException(
                    "Target connection unexpectedly closed while streaming recording");
        }

        try {
            return snapshot.read() != -1;
        } catch (IOException e) {
            return false;
        }
    }

    private boolean shouldRestartRecording(
            RecordingReplace replace, RecordingState state, String recordingName)
            throws BadRequestException {
        switch (replace) {
            case ALWAYS:
                return true;
            case NEVER:
                return false;
            case STOPPED:
                return state == RecordingState.STOPPED;
            default:
                return true;
        }
    }

    @Blocking
    @Transactional
    public ActiveRecording stopRecording(long recordingId, boolean archive) throws Exception {
        // look up the recording by ID to ensure it is an attached entity reference. This is used
        // because this method is invoked from a background scheduler thread, so it is not part of
        // the usual persistence context.
        return stopRecording(ActiveRecording.find("id", recordingId).singleResult(), archive);
    }

    @Blocking
    @Transactional
    public ActiveRecording stopRecording(ActiveRecording recording, boolean archive)
            throws Exception {
        var out =
                connectionManager.executeConnectedTask(
                        recording.target,
                        conn -> {
                            var desc = getDescriptorById(conn, recording.remoteId);
                            if (desc.isEmpty()) {
                                throw new NotFoundException();
                            }
                            if (!desc.get()
                                    .getState()
                                    .equals(
                                            org.openjdk.jmc.rjmx.services.jfr.IRecordingDescriptor
                                                    .RecordingState.STOPPED)) {
                                conn.getService().stop(desc.get());
                            }
                            recording.state = RecordingState.STOPPED;
                            return recording;
                        });
        out.persist();
        if (archive) {
            saveRecording(out);
        }
        return out;
    }

    @Blocking
    @Transactional
    public ActiveRecording stopRecording(ActiveRecording recording) throws Exception {
        return stopRecording(recording, false);
    }

    @Blocking
    @Transactional
    public ActiveRecording deleteRecording(ActiveRecording recording) {
        return connectionManager.executeConnectedTask(
                recording.target,
                conn -> {
                    var desc = getDescriptorById(conn, recording.remoteId);
                    if (desc.isEmpty()) {
                        throw new NotFoundException();
                    }
                    conn.getService().close(desc.get());
                    recording.target.activeRecordings.remove(recording);
                    recording.target.persist();
                    recording.delete();
                    return recording;
                });
    }

    public LinkedRecordingDescriptor toExternalForm(ActiveRecording recording) {
        return new LinkedRecordingDescriptor(
                recording.id,
                recording.remoteId,
                recording.state,
                recording.duration,
                recording.startTime,
                recording.continuous,
                recording.toDisk,
                recording.maxSize,
                recording.maxAge,
                recording.name,
                downloadUrl(recording),
                reportUrl(recording),
                recording.metadata);
    }

    public Pair<String, TemplateType> parseEventSpecifier(String eventSpecifier) {
        if (TEMPLATE_PATTERN.matcher(eventSpecifier).matches()) {
            Matcher m = TEMPLATE_PATTERN.matcher(eventSpecifier);
            m.find();
            String templateName = m.group(1);
            String typeName = m.group(2);
            TemplateType templateType = null;
            if (StringUtils.isNotBlank(typeName)) {
                templateType = TemplateType.valueOf(typeName.toUpperCase());
            }
            return Pair.of(templateName, templateType);
        }
        throw new BadRequestException(eventSpecifier);
    }

    @Blocking
    public Template getPreferredTemplate(
            Target target, String templateName, TemplateType templateType) throws Exception {
        Objects.requireNonNull(target);
        Objects.requireNonNull(templateName);
        if (templateName.equals(EventTemplates.ALL_EVENTS_TEMPLATE.getName())) {
            return EventTemplates.ALL_EVENTS_TEMPLATE;
        }
        Supplier<Optional<Template>> custom =
                () -> {
                    try {
                        return customTemplateService.getTemplates().stream()
                                .filter(t -> t.getName().equals(templateName))
                                .findFirst();
                    } catch (FlightRecorderException e) {
                        logger.error(e);
                        return Optional.empty();
                    }
                };

        Supplier<Optional<Template>> remote =
                () -> {
                    try {
                        return targetTemplateServiceFactory.create(target).getTemplates().stream()
                                .filter(t -> t.getName().equals(templateName))
                                .findFirst();
                    } catch (FlightRecorderException e) {
                        logger.error(e);
                        return Optional.empty();
                    }
                };
        switch (templateType) {
            case TARGET:
                return remote.get().orElseThrow();
            case CUSTOM:
                return custom.get().orElseThrow();
            default:
                return custom.get()
                        .or(() -> remote.get())
                        .orElseThrow(
                                () ->
                                        new BadRequestException(
                                                String.format(
                                                        "Invalid/unknown event template %s",
                                                        templateName)));
        }
    }

    @Blocking
    Optional<IRecordingDescriptor> getDescriptorById(JFRConnection connection, long remoteId) {
        try {
            return connection.getService().getAvailableRecordings().stream()
                    .filter(r -> remoteId == r.getId())
                    .findFirst();
        } catch (Exception e) {
            throw new ServerErrorException(500, e);
        }
    }

    @Blocking
    Optional<IRecordingDescriptor> getDescriptor(
            JFRConnection connection, ActiveRecording activeRecording) {
        return getDescriptorById(connection, activeRecording.remoteId);
    }

    @Blocking
    public Optional<IRecordingDescriptor> getDescriptorByName(
            JFRConnection connection, String recordingName) {
        try {
            return connection.getService().getAvailableRecordings().stream()
                    .filter(r -> Objects.equals(r.getName(), recordingName))
                    .findFirst();
        } catch (Exception e) {
            throw new ServerErrorException(500, e);
        }
    }

    private RecordingState mapState(IRecordingDescriptor desc) {
        switch (desc.getState()) {
            case CREATED:
                return RecordingState.NEW;
            case RUNNING:
                return RecordingState.RUNNING;
            case STOPPING:
                return RecordingState.RUNNING;
            case STOPPED:
                return RecordingState.STOPPED;
            default:
                logger.warnv("Unrecognized recording state: {0}", desc.getState());
                return RecordingState.CLOSED;
        }
    }

    @Blocking
    public List<S3Object> listArchivedRecordingObjects() {
        return listArchivedRecordingObjects(null);
    }

    @Blocking
    public List<S3Object> listArchivedRecordingObjects(String jvmId) {
        var builder = ListObjectsV2Request.builder().bucket(archiveBucket);
        if (StringUtils.isNotBlank(jvmId)) {
            builder = builder.prefix(jvmId);
        }
        return storage.listObjectsV2(builder.build()).contents().stream()
                .filter(
                        o -> {
                            var metadata = getArchivedRecordingMetadata(o.key());
                            var temporary = metadata.map(m -> m.expiry() != null).orElse(false);
                            return !temporary;
                        })
                .toList();
    }

    public String saveRecording(ActiveRecording recording) throws Exception {
        return saveRecording(recording, null);
    }

    public String saveRecording(ActiveRecording recording, Instant expiry) throws Exception {
        return saveRecording(recording, null, expiry);
    }

    @Blocking
    public String saveRecording(ActiveRecording recording, String savename, Instant expiry)
            throws Exception {
        // AWS object key name guidelines advise characters to avoid (% so we should not pass url
        // encoded characters)
        String transformedAlias =
                URLDecoder.decode(recording.target.alias, StandardCharsets.UTF_8)
                        .replaceAll("[\\._/]+", "-");
        String timestamp =
                clock.now().truncatedTo(ChronoUnit.SECONDS).toString().replaceAll("[-:]+", "");
        String filename =
                String.format("%s_%s_%s.jfr", transformedAlias, recording.name, timestamp);
        if (StringUtils.isBlank(savename)) {
            savename = filename;
        }
        int mib = 1024 * 1024;
        String key = archivedRecordingKey(recording.target.jvmId, filename);
        String multipartId = null;
        List<Pair<Integer, String>> parts = new ArrayList<>();
        try (var stream = remoteRecordingStreamFactory.open(recording);
                var ch = Channels.newChannel(stream)) {
            ByteBuffer buf = ByteBuffer.allocate(20 * mib);
            CreateMultipartUploadRequest.Builder builder =
                    CreateMultipartUploadRequest.builder()
                            .bucket(archiveBucket)
                            .key(key)
                            .contentType(JFR_MIME)
                            .contentDisposition(
                                    String.format("attachment; filename=\"%s\"", savename))
                            .tagging(createActiveRecordingTagging(recording, expiry));
            if (expiry != null && expiry.isAfter(Instant.now())) {
                builder = builder.expires(expiry);
            }
            CreateMultipartUploadRequest request = builder.build();
            multipartId = storage.createMultipartUpload(request).uploadId();
            int read = 0;
            long accum = 0;
            for (int i = 1; i <= 10_000; i++) {
                read = ch.read(buf);

                if (read == 0) {
                    read = retryRead(ch, buf);
                }
                accum += read;
                if (read == -1) {
                    logger.infov("Completed upload of {0} chunks ({1} bytes)", i - 1, accum + 1);
                    logger.infov("Key: {0}", key);
                    break;
                }

                logger.infov("Writing chunk {0} of {1} bytes", i, read);
                String eTag =
                        storage.uploadPart(
                                        UploadPartRequest.builder()
                                                .bucket(archiveBucket)
                                                .key(key)
                                                .uploadId(multipartId)
                                                .partNumber(i)
                                                .contentLength(Long.valueOf(read))
                                                .build(),
                                        RequestBody.fromByteBuffer(buf.slice(0, read)))
                                .eTag();
                parts.add(Pair.of(i, eTag));
                buf.clear();
                // S3 API limit
                if (i == 10_000) {
                    throw new IndexOutOfBoundsException("Exceeded S3 maximum part count");
                }
            }
        } catch (Exception e) {
            logger.error("Could not upload recording to S3 storage", e);
            try {
                if (multipartId != null) {
                    storage.abortMultipartUpload(
                            AbortMultipartUploadRequest.builder()
                                    .bucket(archiveBucket)
                                    .key(key)
                                    .uploadId(multipartId)
                                    .build());
                }
            } catch (Exception e2) {
                logger.error("Could not abort S3 multipart upload", e2);
            }
            throw e;
        }
        try {
            storage.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder()
                            .bucket(archiveBucket)
                            .key(key)
                            .uploadId(multipartId)
                            .multipartUpload(
                                    CompletedMultipartUpload.builder()
                                            .parts(
                                                    parts.stream()
                                                            .map(
                                                                    part ->
                                                                            CompletedPart.builder()
                                                                                    .partNumber(
                                                                                            part
                                                                                                    .getLeft())
                                                                                    .eTag(
                                                                                            part
                                                                                                    .getRight())
                                                                                    .build())
                                                            .toList())
                                            .build())
                            .build());
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            throw e;
        }
        if (expiry == null) {
            var event =
                    new ActiveRecordingEvent(
                            Recordings.RecordingEventCategory.ACTIVE_SAVED,
                            ActiveRecordingEvent.Payload.of(this, recording));
            bus.publish(event.category().category(), event.payload().recording());
            bus.publish(
                    MessagingServer.class.getName(),
                    new Notification(event.category().category(), event.payload()));
        }
        return filename;
    }

    public Optional<Metadata> getArchivedRecordingMetadata(String jvmId, String filename) {
        return getArchivedRecordingMetadata(archivedRecordingKey(jvmId, filename));
    }

    @Blocking
    public Optional<Metadata> getArchivedRecordingMetadata(String storageKey) {
        try {
            return Optional.of(
                    taggingToMetadata(
                            storage.getObjectTagging(
                                            GetObjectTaggingRequest.builder()
                                                    .bucket(archiveBucket)
                                                    .key(storageKey)
                                                    .build())
                                    .tagSet()));
        } catch (NoSuchKeyException nske) {
            logger.warn(nske);
            return Optional.empty();
        }
    }

    private String decodeBase64(String encoded) {
        return new String(base64Url.decode(encoded), StandardCharsets.UTF_8);
    }

    public String archivedRecordingKey(String jvmId, String filename) {
        return (jvmId + "/" + filename).strip();
    }

    public String archivedRecordingKey(Pair<String, String> pair) {
        return archivedRecordingKey(pair.getKey(), pair.getValue());
    }

    public String encodedKey(String jvmId, String filename) {
        Objects.requireNonNull(jvmId);
        Objects.requireNonNull(filename);
        return base64Url.encodeAsString(
                (archivedRecordingKey(jvmId, filename)).getBytes(StandardCharsets.UTF_8));
    }

    // TODO refactor this and encapsulate archived recording keys as a record with override toString
    public Pair<String, String> decodedKey(String encodedKey) {
        String key = new String(base64Url.decode(encodedKey), StandardCharsets.UTF_8);
        String[] parts = key.split("/");
        return Pair.of(parts[0], parts[1]);
    }

    public InputStream getActiveInputStream(ActiveRecording recording) throws Exception {
        return remoteRecordingStreamFactory.open(recording);
    }

    public InputStream getActiveInputStream(long targetId, long remoteId) throws Exception {
        var target = Target.<Target>findById(targetId);
        var recording = target.getRecordingById(remoteId);
        var stream = remoteRecordingStreamFactory.open(recording);
        return stream;
    }

    public InputStream getArchivedRecordingStream(String jvmId, String recordingName) {
        return getArchivedRecordingStream(encodedKey(jvmId, recordingName));
    }

    @Blocking
    public InputStream getArchivedRecordingStream(String encodedKey) {
        String key = new String(base64Url.decode(encodedKey), StandardCharsets.UTF_8);

        GetObjectRequest getRequest =
                GetObjectRequest.builder().bucket(archiveBucket).key(key).build();

        return storage.getObject(getRequest);
    }

    public String downloadUrl(ActiveRecording recording) {
        return String.format("/api/v3/activedownload/%d", recording.id);
    }

    public String downloadUrl(String jvmId, String filename) {
        return String.format("/api/v3/download/%s", encodedKey(jvmId, filename));
    }

    public String reportUrl(ActiveRecording recording) {
        return String.format(
                "/api/v3/targets/%d/reports/%d", recording.target.id, recording.remoteId);
    }

    public String reportUrl(String jvmId, String filename) {
        return String.format("/api/v3/reports/%s", encodedKey(jvmId, filename));
    }

    @Blocking
    private int retryRead(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
        int attempts = 30;
        int read = 0;

        while (attempts > 0) {
            logger.info("No bytes read, retrying...");
            read = channel.read(buffer);
            if (read > 0 || read < 0) {
                break;
            } else {
                attempts--;
            }
        }

        if (read == 0) {
            throw new IOException("No bytes read after 30 retry attempts");
        }

        return read;
    }

    @Blocking
    void safeCloseRecording(JFRConnection conn, IRecordingDescriptor rec) {
        try {
            conn.getService().close(rec);
        } catch (org.openjdk.jmc.rjmx.services.jfr.FlightRecorderException e) {
            logger.error("Failed to stop remote recording", e);
        } catch (Exception e) {
            logger.error("Unexpected exception", e);
        }
    }

    /* Archived Recording Helpers */
    @Blocking
    public void deleteArchivedRecording(String jvmId, String filename) {
        storage.deleteObject(
                DeleteObjectRequest.builder()
                        .bucket(archiveBucket)
                        .key(archivedRecordingKey(jvmId, filename))
                        .build());

        var metadata = Metadata.empty(); // TODO
        var target = Target.getTargetByJvmId(jvmId);
        var event =
                new ArchivedRecordingEvent(
                        Recordings.RecordingEventCategory.ARCHIVED_DELETED,
                        ArchivedRecordingEvent.Payload.of(
                                target.map(t -> t.connectUrl).orElse(null),
                                new ArchivedRecording(
                                        filename,
                                        downloadUrl(jvmId, filename),
                                        reportUrl(jvmId, filename),
                                        metadata,
                                        0,
                                        0)));
        bus.publish(event.category().category(), event.payload().recording());
        bus.publish(
                MessagingServer.class.getName(),
                new Notification(event.category().category(), event.payload()));
    }

    Tagging createActiveRecordingTagging(ActiveRecording recording, Instant expiry) {
        Map<String, String> labels = new HashMap<>(recording.metadata.labels());
        labels.put("connectUrl", recording.target.connectUrl.toString());
        labels.put("jvmId", recording.target.jvmId);
        Metadata metadata = new Metadata(labels, expiry);
        return createMetadataTagging(metadata);
    }

    // Metadata
    Tagging createMetadataTagging(Metadata metadata) {
        // TODO attach other metadata than labels somehow. Prefixed keys to create partitioning?
        var tags = new ArrayList<Tag>();
        tags.addAll(
                metadata.labels().entrySet().stream()
                        .map(
                                e ->
                                        Tag.builder()
                                                .key(
                                                        base64Url.encodeAsString(
                                                                e.getKey()
                                                                        .getBytes(
                                                                                StandardCharsets
                                                                                        .UTF_8)))
                                                .value(
                                                        base64Url.encodeAsString(
                                                                e.getValue()
                                                                        .getBytes(
                                                                                StandardCharsets
                                                                                        .UTF_8)))
                                                .build())
                        .toList());
        if (metadata.expiry() != null) {
            tags.add(
                    Tag.builder()
                            .key(
                                    base64Url.encodeAsString(
                                            objectExpirationLabel.getBytes(StandardCharsets.UTF_8)))
                            .value(
                                    base64Url.encodeAsString(
                                            metadata.expiry()
                                                    .atOffset(ZoneOffset.UTC)
                                                    .toString()
                                                    .getBytes(StandardCharsets.UTF_8)))
                            .build());
        }
        return Tagging.builder().tagSet(tags).build();
    }

    private Metadata taggingToMetadata(List<Tag> tagSet) {
        // TODO parse out other metadata than labels
        Instant expiry = null;
        var labels = new HashMap<String, String>();
        for (var tag : tagSet) {
            var key = decodeBase64(tag.key());
            var value = decodeBase64(tag.value());
            if (key.equals(objectExpirationLabel)) {
                expiry = Instant.parse(value);
            } else {
                labels.put(key, value);
            }
        }
        return new Metadata(labels, expiry);
    }

    @Blocking
    public Uni<String> uploadToJFRDatasource(long targetEntityId, long remoteId) throws Exception {
        Target target = Target.getTargetById(targetEntityId);
        Objects.requireNonNull(target, "Target from targetId not found");
        ActiveRecording recording = target.getRecordingById(remoteId);
        Objects.requireNonNull(recording, "ActiveRecording from remoteId not found");
        Path recordingPath =
                connectionManager.executeConnectedTask(
                        target,
                        connection -> {
                            return getRecordingCopyPath(connection, target, recording.name)
                                    .orElseThrow(
                                            () ->
                                                    new RecordingNotFoundException(
                                                            target.targetId(), recording.name));
                        });

        return uploadToJFRDatasource(recordingPath);
    }

    @Blocking
    public Uni<String> uploadToJFRDatasource(Pair<String, String> key) throws Exception {
        Objects.requireNonNull(key);
        Objects.requireNonNull(key.getKey());
        Objects.requireNonNull(key.getValue());
        GetObjectRequest getRequest =
                GetObjectRequest.builder()
                        .bucket(archiveBucket)
                        .key(archivedRecordingKey(key))
                        .build();

        Path recordingPath = fs.createTempFile(null, null);
        // the S3 client will create the file at this path, we just need to get a fresh temp file
        // path but one that does not yet exist
        fs.deleteIfExists(recordingPath);

        storage.getObject(getRequest, recordingPath);

        return uploadToJFRDatasource(recordingPath);
    }

    private Uni<String> uploadToJFRDatasource(Path recordingPath)
            throws URISyntaxException, InterruptedException, ExecutionException {
        MultipartForm form =
                MultipartForm.create()
                        .binaryFileUpload(
                                "file", DATASOURCE_FILENAME, recordingPath.toString(), JFR_MIME);

        var asyncRequest =
                webClient
                        .postAbs(
                                grafanaDatasourceURL
                                        .get()
                                        .toURI()
                                        .resolve("/load")
                                        .normalize()
                                        .toString())
                        .addQueryParam("overwrite", "true")
                        .timeout(connectionFailedTimeout.toMillis())
                        .sendMultipartForm(form);
        return asyncRequest
                .onItem()
                .transform(HttpResponse::bodyAsString)
                .eventually(
                        () -> {
                            try {
                                fs.deleteIfExists(recordingPath);
                            } catch (IOException e) {
                                logger.warn(e);
                            }
                        });
    }

    @Blocking
    Optional<Path> getRecordingCopyPath(
            JFRConnection connection, Target target, String recordingName) throws Exception {
        return connection.getService().getAvailableRecordings().stream()
                .filter(recording -> recording.getName().equals(recordingName))
                .findFirst()
                .map(
                        descriptor -> {
                            try {
                                Path tempFile = fs.createTempFile(null, null);
                                try (var stream =
                                        remoteRecordingStreamFactory.open(
                                                connection, target, descriptor)) {
                                    fs.copy(stream, tempFile, StandardCopyOption.REPLACE_EXISTING);
                                }
                                return tempFile;
                            } catch (Exception e) {
                                logger.error(e);
                                throw new BadRequestException(e);
                            }
                        });
    }

    public enum RecordingReplace {
        ALWAYS,
        NEVER,
        STOPPED;

        public static RecordingReplace fromString(String replace) {
            for (RecordingReplace r : RecordingReplace.values()) {
                if (r.name().equalsIgnoreCase(replace)) {
                    return r;
                }
            }
            throw new IllegalArgumentException("Invalid recording replace value: " + replace);
        }
    }

    static class StopRecordingJob implements Job {

        @Inject RecordingHelper recordingHelper;
        @Inject Logger logger;

        @Override
        @Transactional
        public void execute(JobExecutionContext ctx) throws JobExecutionException {
            var recording = (ActiveRecording) ctx.getJobDetail().getJobDataMap().get("recording");
            var archive = (boolean) ctx.getJobDetail().getJobDataMap().get("archive");
            try {
                recordingHelper.stopRecording(recording.id, archive);
            } catch (Exception e) {
                throw new JobExecutionException(e);
            }
        }
    }

    static class RecordingNotFoundException extends Exception {
        public RecordingNotFoundException(String targetId, String recordingName) {
            super(
                    String.format(
                            "Recording %s was not found in the target [%s].",
                            recordingName, targetId));
        }

        public RecordingNotFoundException(Pair<String, String> key) {
            this(key.getLeft(), key.getRight());
        }
    }

    static class SnapshotCreationException extends Exception {
        public SnapshotCreationException(String message) {
            super(message);
        }
    }
}
