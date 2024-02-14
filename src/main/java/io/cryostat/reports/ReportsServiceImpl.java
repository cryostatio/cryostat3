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
package io.cryostat.reports;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.openjdk.jmc.flightrecorder.rules.IRule;

import io.cryostat.ConfigProperties;
import io.cryostat.core.reports.InterruptibleReportGenerator;
import io.cryostat.core.reports.InterruptibleReportGenerator.AnalysisResult;
import io.cryostat.recordings.ActiveRecording;
import io.cryostat.recordings.RecordingHelper;
import io.cryostat.util.HttpStatusCodeIdentifier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.handler.HttpException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

@ApplicationScoped
class ReportsServiceImpl implements ReportsService {

    @ConfigProperty(name = ConfigProperties.REPORTS_SIDECAR_URL)
    Optional<URI> sidecarUri;

    @ConfigProperty(name = ConfigProperties.AWS_BUCKET_NAME_ARCHIVES)
    String archiveBucket;

    @ConfigProperty(name = ConfigProperties.STORAGE_EXT_URL)
    Optional<String> externalStorageUrl;

    @Inject ObjectMapper mapper;
    @Inject RecordingHelper helper;
    @Inject InterruptibleReportGenerator reportGenerator;
    @Inject S3Presigner presigner;
    @Inject Logger logger;

    CloseableHttpClient http;

    void onStart(@Observes StartupEvent evt) {
        this.http = HttpClients.createSystem();
    }

    void onStop(@Observes ShutdownEvent evt) throws IOException {
        this.http.close();
    }

    @Blocking
    @Override
    public Uni<Map<String, AnalysisResult>> reportFor(
            ActiveRecording recording, Predicate<IRule> predicate) {
        Future<Map<String, AnalysisResult>> future =
                sidecarUri
                        .map(
                                uri -> {
                                    logger.tracev(
                                            "sidecar reportFor active recording {0} {1}",
                                            recording.target.jvmId, recording.remoteId);
                                    try {
                                        return fireRequest(uri, getPresignedUri(recording));
                                    } catch (Exception e) {
                                        throw new ReportGenerationException(e);
                                    }
                                })
                        .orElseGet(
                                () -> {
                                    logger.tracev(
                                            "inprocess reportFor active recording {0} {1}",
                                            recording.target.jvmId, recording.remoteId);
                                    try {
                                        return process(
                                                helper.getActiveInputStream(recording), predicate);
                                    } catch (Exception e) {
                                        throw new ReportGenerationException(e);
                                    }
                                });
        return Uni.createFrom().future(future);
    }

    @Blocking
    @Override
    public Uni<Map<String, AnalysisResult>> reportFor(
            String jvmId, String filename, Predicate<IRule> predicate) {
        Future<Map<String, AnalysisResult>> future =
                sidecarUri
                        .map(
                                uri -> {
                                    logger.tracev(
                                            "sidecar reportFor archived recording {0} {1}",
                                            jvmId, filename);
                                    try {
                                        return fireRequest(uri, getPresignedPath(jvmId, filename));
                                    } catch (Exception e) {
                                        throw new ReportGenerationException(e);
                                    }
                                })
                        .orElseGet(
                                () -> {
                                    logger.tracev(
                                            "inprocess reportFor archived recording {0} {1}",
                                            jvmId, filename);
                                    return process(
                                            helper.getArchivedRecordingStream(jvmId, filename),
                                            predicate);
                                });

        return Uni.createFrom().future(future);
    }

    private Future<Map<String, AnalysisResult>> process(
            InputStream stream, Predicate<IRule> predicate) {
        return reportGenerator.generateEvalMapInterruptibly(
                new BufferedInputStream(stream), predicate);
    }

    private URI getPresignedUri(ActiveRecording recording) throws Exception {
        // TODO refactor, this is copied out of Recordings.java
        String savename = recording.name;
        String filename = helper.saveRecording(recording, savename, Instant.now().plusSeconds(60));
        return getPresignedPath(recording.target.jvmId, filename);
    }

    private URI getPresignedPath(String jvmId, String filename) throws URISyntaxException {
        // TODO refactor, this is copied out of Recordings.java
        logger.infov("Handling presigned download request for {0}/{1}", jvmId, filename);
        GetObjectRequest getRequest =
                GetObjectRequest.builder()
                        .bucket(archiveBucket)
                        .key(helper.archivedRecordingKey(Pair.of(jvmId, filename)))
                        .build();
        GetObjectPresignRequest presignRequest =
                GetObjectPresignRequest.builder()
                        .signatureDuration(Duration.ofMinutes(1))
                        .getObjectRequest(getRequest)
                        .build();
        return URI.create(presigner.presignGetObject(presignRequest).url().toString()).normalize();
    }

    private Future<Map<String, AnalysisResult>> fireRequest(
            URI sidecarUri, URI presignedRecordingUri) {
        var cf = new CompletableFuture<Map<String, AnalysisResult>>();
        try {
            var post = new HttpPost(sidecarUri.resolve("remote_report"));
            var form =
                    MultipartEntityBuilder.create()
                            .addTextBody("path", presignedRecordingUri.getPath())
                            .addTextBody("query", presignedRecordingUri.getQuery())
                            .build();
            post.setEntity(form);
            http.execute(
                    post,
                    response -> {
                        if (!HttpStatusCodeIdentifier.isSuccessCode(response.getCode())) {
                            cf.completeExceptionally(
                                    new HttpException(
                                            response.getCode(), response.getReasonPhrase()));
                            return null;
                        }
                        var entity = response.getEntity();
                        Map<String, AnalysisResult> evaluation =
                                mapper.readValue(
                                        entity.getContent(),
                                        new TypeReference<Map<String, AnalysisResult>>() {});
                        cf.complete(evaluation);
                        return null;
                    });
        } catch (Exception e) {
            cf.completeExceptionally(new ReportGenerationException(e));
        }
        return cf;
    }

    public static class ReportGenerationException extends RuntimeException {
        public ReportGenerationException(Throwable cause) {
            super(cause);
        }
    }
}
