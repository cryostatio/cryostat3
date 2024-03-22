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
package io.cryostat.graphql;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import org.openjdk.jmc.common.unit.QuantityConversionException;

import io.cryostat.core.templates.Template;
import io.cryostat.core.templates.TemplateType;
import io.cryostat.discovery.DiscoveryNode;
import io.cryostat.graphql.RootNode.DiscoveryNodeFilter;
import io.cryostat.graphql.TargetNodes.AggregateInfo;
import io.cryostat.graphql.TargetNodes.Recordings;
import io.cryostat.graphql.matchers.LabelSelectorMatcher;
import io.cryostat.recordings.ActiveRecording;
import io.cryostat.recordings.RecordingHelper;
import io.cryostat.recordings.RecordingHelper.RecordingOptions;
import io.cryostat.recordings.RecordingHelper.RecordingReplace;
import io.cryostat.recordings.Recordings.ArchivedRecording;
import io.cryostat.targets.Target;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.graphql.api.Nullable;
import io.smallrye.graphql.execution.ExecutionException;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jdk.jfr.RecordingState;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Mutation;
import org.eclipse.microprofile.graphql.NonNull;
import org.eclipse.microprofile.graphql.Source;
import org.jboss.logging.Logger;

@GraphQLApi
public class ActiveRecordings {

    @Inject RecordingHelper recordingHelper;
    @Inject Logger logger;

    @Blocking
    @Transactional
    @Mutation
    @Description(
            "Start a new Flight Recording on all Targets under the subtrees of the discovery nodes"
                    + " matching the given filter")
    public List<ActiveRecording> createRecording(
            @NonNull DiscoveryNodeFilter nodes, @NonNull RecordingSettings recording) {
        return DiscoveryNode.<DiscoveryNode>listAll().stream()
                .filter(nodes)
                .flatMap(
                        node ->
                                RootNode.recurseChildren(node, n -> n.target != null).stream()
                                        .map(n -> n.target))
                .map(
                        target -> {
                            var template =
                                    recordingHelper.getPreferredTemplate(
                                            target,
                                            recording.template,
                                            TemplateType.valueOf(recording.templateType));
                            try {
                                return recordingHelper
                                        .startRecording(
                                                target,
                                                Optional.ofNullable(recording.replace)
                                                        .map(RecordingReplace::valueOf)
                                                        .orElse(RecordingReplace.STOPPED),
                                                template,
                                                recording.asOptions(),
                                                Optional.ofNullable(recording.metadata)
                                                        .map(s -> s.labels)
                                                        .orElse(Map.of()))
                                        .await()
                                        .atMost(Duration.ofSeconds(10));
                            } catch (QuantityConversionException qce) {
                                throw new ExecutionException(qce);
                            }
                        })
                .toList();
    }

    @Blocking
    @Transactional
    @Mutation
    @Description(
            "Archive an existing Flight Recording matching the given filter, on all Targets under"
                    + " the subtrees of the discovery nodes matching the given filter")
    public List<ArchivedRecording> archiveRecording(
            @NonNull DiscoveryNodeFilter nodes, @Nullable ActiveRecordingsFilter recordings) {
        return DiscoveryNode.<DiscoveryNode>listAll().stream()
                .filter(nodes)
                .flatMap(
                        node ->
                                RootNode.recurseChildren(node, n -> n.target != null).stream()
                                        .map(n -> n.target))
                .flatMap(
                        t ->
                                t.activeRecordings.stream()
                                        .filter(r -> recordings == null || recordings.test(r)))
                .map(
                        recording -> {
                            try {
                                return recordingHelper.archiveRecording(recording, null, null);
                            } catch (Exception e) {
                                throw new ExecutionException(e);
                            }
                        })
                .toList();
    }

    @Blocking
    @Transactional
    @Mutation
    @Description(
            "Stop an existing Flight Recording matching the given filter, on all Targets under"
                    + " the subtrees of the discovery nodes matching the given filter")
    public List<ActiveRecording> stopRecording(
            @NonNull DiscoveryNodeFilter nodes, @Nullable ActiveRecordingsFilter recordings) {
        return DiscoveryNode.<DiscoveryNode>listAll().stream()
                .filter(nodes)
                .flatMap(
                        node ->
                                RootNode.recurseChildren(node, n -> n.target != null).stream()
                                        .map(n -> n.target))
                .flatMap(
                        t ->
                                t.activeRecordings.stream()
                                        .filter(r -> recordings == null || recordings.test(r)))
                .map(
                        recording -> {
                            try {
                                return recordingHelper
                                        .stopRecording(recording)
                                        .await()
                                        .atMost(Duration.ofSeconds(10));
                            } catch (Exception e) {
                                throw new ExecutionException(e);
                            }
                        })
                .toList();
    }

    @Blocking
    @Transactional
    @Mutation
    @Description(
            "Delete an existing Flight Recording matching the given filter, on all Targets under"
                    + " the subtrees of the discovery nodes matching the given filter")
    public List<ActiveRecording> deleteRecording(
            @NonNull DiscoveryNodeFilter nodes, @Nullable ActiveRecordingsFilter recordings) {
        var activeRecordings =
                DiscoveryNode.<DiscoveryNode>listAll().stream()
                        .filter(nodes)
                        .flatMap(
                                node ->
                                        RootNode.recurseChildren(node, n -> n.target != null)
                                                .stream()
                                                .map(n -> n.target))
                        .flatMap(
                                t ->
                                        t.activeRecordings.stream()
                                                .filter(
                                                        r ->
                                                                recordings == null
                                                                        || recordings.test(r)))
                        .toList();
        return activeRecordings.stream()
                .map(
                        recording -> {
                            try {
                                return recordingHelper
                                        .deleteRecording(recording)
                                        .await()
                                        .atMost(Duration.ofSeconds(10));
                            } catch (Exception e) {
                                throw new ExecutionException(e);
                            }
                        })
                .toList();
    }

    @Blocking
    @Transactional
    @Mutation
    @Description(
            "Create a Flight Recorder Snapshot on all Targets under"
                    + " the subtrees of the discovery nodes matching the given filter")
    public List<ActiveRecording> createSnapshot(@NonNull DiscoveryNodeFilter nodes) {
        return DiscoveryNode.<DiscoveryNode>listAll().stream()
                .filter(nodes)
                .flatMap(
                        node ->
                                RootNode.recurseChildren(node, n -> n.target != null).stream()
                                        .map(n -> n.target))
                .map(
                        target -> {
                            try {
                                return recordingHelper
                                        .createSnapshot(target)
                                        .await()
                                        .atMost(Duration.ofSeconds(10));
                            } catch (Exception e) {
                                throw new ExecutionException(e);
                            }
                        })
                .toList();
    }

    @Blocking
    @Transactional
    @Description("Start a new Flight Recording on the specified Target")
    public Uni<ActiveRecording> doStartRecording(
            @Source Target target, @NonNull RecordingSettings recording)
            throws QuantityConversionException {
        var fTarget = Target.<Target>findById(target.id);
        Template template =
                recordingHelper.getPreferredTemplate(
                        fTarget, recording.template, TemplateType.valueOf(recording.templateType));
        return recordingHelper.startRecording(
                fTarget,
                Optional.ofNullable(recording.replace)
                        .map(RecordingReplace::valueOf)
                        .orElse(RecordingReplace.STOPPED),
                template,
                recording.asOptions(),
                Optional.ofNullable(recording.metadata).map(s -> s.labels).orElse(Map.of()));
    }

    @Blocking
    @Transactional
    @Description("Create a new Flight Recorder Snapshot on the specified Target")
    public Uni<ActiveRecording> doSnapshot(@Source Target target) {
        var fTarget = Target.<Target>findById(target.id);
        return recordingHelper.createSnapshot(fTarget);
    }

    @Blocking
    @Transactional
    @Description("Stop the specified Flight Recording")
    public Uni<ActiveRecording> doStop(@Source ActiveRecording recording) {
        var ar = ActiveRecording.<ActiveRecording>findById(recording.id);
        return recordingHelper.stopRecording(ar);
    }

    @Blocking
    @Transactional
    @Description("Delete the specified Flight Recording")
    public Uni<ActiveRecording> doDelete(@Source ActiveRecording recording) {
        var ar = ActiveRecording.<ActiveRecording>findById(recording.id);
        return recordingHelper.deleteRecording(ar);
    }

    @Blocking
    @Transactional
    @Description("Archive the specified Flight Recording")
    public Uni<ArchivedRecording> doArchive(@Source ActiveRecording recording) throws Exception {
        var ar = ActiveRecording.<ActiveRecording>findById(recording.id);
        return Uni.createFrom().item(recordingHelper.archiveRecording(ar, null, null));
    }

    public TargetNodes.ActiveRecordings active(
            @Source Recordings recordings, ActiveRecordingsFilter filter) {
        var out = new TargetNodes.ActiveRecordings();
        out.data = new ArrayList<>();
        out.aggregate = AggregateInfo.empty();

        var in = recordings.active;
        if (in != null && in.data != null) {
            out.data =
                    in.data.stream().filter(r -> filter == null ? true : filter.test(r)).toList();
            out.aggregate = AggregateInfo.fromActive(out.data);
        }

        return out;
    }

    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public static class RecordingSettings {
        public @NonNull String name;
        public @NonNull String template;
        public @NonNull String templateType;
        public @Nullable String replace;
        public @Nullable Boolean continuous;
        public @Nullable Boolean archiveOnStop;
        public @Nullable Boolean toDisk;
        public @Nullable Long duration;
        public @Nullable Long maxSize;
        public @Nullable Long maxAge;
        public @Nullable RecordingMetadata metadata;

        public RecordingOptions asOptions() {
            return new RecordingOptions(
                    name,
                    Optional.ofNullable(toDisk),
                    Optional.ofNullable(archiveOnStop),
                    Optional.ofNullable(duration),
                    Optional.ofNullable(maxSize),
                    Optional.ofNullable(maxAge));
        }
    }

    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public static class RecordingMetadata {
        public @Nullable Map<String, String> labels;
    }

    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public static class ActiveRecordingsFilter implements Predicate<ActiveRecording> {
        public @Nullable String name;
        public @Nullable List<String> names;
        public @Nullable List<String> labels;
        public @Nullable RecordingState state;
        public @Nullable Boolean continuous;
        public @Nullable Boolean toDisk;
        public @Nullable Long durationMsGreaterThanEqual;
        public @Nullable Long durationMsLessThanEqual;
        public @Nullable Long startTimeMsAfterEqual;
        public @Nullable Long startTimeMsBeforeEqual;

        @Override
        public boolean test(ActiveRecording r) {
            Predicate<ActiveRecording> matchesName =
                    n -> name == null || Objects.equals(name, n.name);
            Predicate<ActiveRecording> matchesNames = n -> names == null || names.contains(n.name);
            Predicate<ActiveRecording> matchesLabels =
                    n ->
                            labels == null
                                    || labels.stream()
                                            .allMatch(
                                                    label ->
                                                            LabelSelectorMatcher.parse(label)
                                                                    .test(n.metadata.labels()));
            Predicate<ActiveRecording> matchesState = n -> state == null || n.state.equals(state);
            Predicate<ActiveRecording> matchesContinuous =
                    n -> continuous == null || continuous.equals(n.continuous);
            Predicate<ActiveRecording> matchesToDisk =
                    n -> toDisk == null || toDisk.equals(n.toDisk);
            Predicate<ActiveRecording> matchesDurationGte =
                    n ->
                            durationMsGreaterThanEqual == null
                                    || durationMsGreaterThanEqual >= n.duration;
            Predicate<ActiveRecording> matchesDurationLte =
                    n -> durationMsLessThanEqual == null || durationMsLessThanEqual <= n.duration;
            Predicate<ActiveRecording> matchesStartTimeAfter =
                    n -> startTimeMsAfterEqual == null || startTimeMsAfterEqual >= n.startTime;
            Predicate<ActiveRecording> matchesStartTimeBefore =
                    n -> startTimeMsBeforeEqual == null || startTimeMsBeforeEqual <= n.startTime;

            return List.of(
                            matchesName,
                            matchesNames,
                            matchesLabels,
                            matchesState,
                            matchesContinuous,
                            matchesToDisk,
                            matchesDurationGte,
                            matchesDurationLte,
                            matchesStartTimeBefore,
                            matchesStartTimeAfter)
                    .stream()
                    .reduce(x -> true, Predicate::and)
                    .test(r);
        }
    }
}
