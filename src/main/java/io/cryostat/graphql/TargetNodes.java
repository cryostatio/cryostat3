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

import java.util.List;

import io.cryostat.core.net.JFRConnection;
import io.cryostat.core.net.MBeanMetrics;
import io.cryostat.discovery.DiscoveryNode;
import io.cryostat.graphql.ActiveRecordings.ActiveRecordingsFilter;
import io.cryostat.graphql.ArchivedRecordings.ArchivedRecordingsFilter;
import io.cryostat.graphql.RootNode.DiscoveryNodeFilter;
import io.cryostat.recordings.ActiveRecording;
import io.cryostat.recordings.RecordingHelper;
import io.cryostat.recordings.Recordings.ArchivedRecording;
import io.cryostat.targets.Target;
import io.cryostat.targets.TargetConnectionManager;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import graphql.schema.DataFetchingEnvironment;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.graphql.api.Context;
import io.smallrye.graphql.api.Nullable;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.NonNull;
import org.eclipse.microprofile.graphql.Query;
import org.eclipse.microprofile.graphql.Source;

@GraphQLApi
public class TargetNodes {

    @Inject RecordingHelper recordingHelper;
    @Inject TargetConnectionManager connectionManager;

    @Blocking
    @Query("targetNodes")
    @Description("Get the Target discovery nodes, i.e. the leaf nodes of the discovery tree")
    public List<DiscoveryNode> getTargetNodes(DiscoveryNodeFilter filter) {
        // TODO do this filtering at the database query level as much as possible. As is, this will
        // load the entire discovery tree out of the database, then perform the filtering at the
        // application level.
        return Target.<Target>findAll().stream()
                // FIXME filtering by distinct JVM ID breaks clients that expect to be able to use a
                // different connection URL (in the node filter or for client-side filtering) than
                // the one we end up selecting for here.
                // .filter(distinctWith(t -> t.jvmId))
                .map(t -> t.discoveryNode)
                .filter(n -> filter == null ? true : filter.test(n))
                .toList();
    }

    // private static <T> Predicate<T> distinctWith(Function<? super T, ?> fn) {
    //     Set<Object> observed = ConcurrentHashMap.newKeySet();
    //     return t -> observed.add(fn.apply(t));
    // }

    @Blocking
    public ActiveRecordings activeRecordings(
            @Source Target target, @Nullable ActiveRecordingsFilter filter) {
        var fTarget = Target.<Target>findById(target.id);
        var recordings = new ActiveRecordings();
        recordings.data =
                fTarget.activeRecordings.stream()
                        .filter(r -> filter == null || filter.test(r))
                        .toList();
        recordings.aggregate = AggregateInfo.fromActive(recordings.data);
        return recordings;
    }

    @Blocking
    public ArchivedRecordings archivedRecordings(
            @Source Target target, @Nullable ArchivedRecordingsFilter filter) {
        var fTarget = Target.<Target>findById(target.id);
        var recordings = new ArchivedRecordings();
        recordings.data =
                recordingHelper.listArchivedRecordings(fTarget).stream()
                        .filter(r -> filter == null || filter.test(r))
                        .toList();
        recordings.aggregate = AggregateInfo.fromArchived(recordings.data);
        return recordings;
    }

    @Blocking
    @Description("Get the active and archived recordings belonging to this target")
    public Recordings recordings(@Source Target target, Context context) {
        var fTarget = Target.<Target>findById(target.id);
        var dfe = context.unwrap(DataFetchingEnvironment.class);
        var requestedFields =
                dfe.getSelectionSet().getFields().stream().map(field -> field.getName()).toList();

        var recordings = new Recordings();

        if (requestedFields.contains("active")) {
            recordings.active = new ActiveRecordings();
            recordings.active.data = fTarget.activeRecordings;
            recordings.active.aggregate = AggregateInfo.fromActive(recordings.active.data);
        }

        if (requestedFields.contains("archived")) {
            recordings.archived = new ArchivedRecordings();
            recordings.archived.data = recordingHelper.listArchivedRecordings(fTarget);
            recordings.archived.aggregate = AggregateInfo.fromArchived(recordings.archived.data);
        }

        return recordings;
    }

    @Blocking
    @Description("Get live MBean metrics snapshot from the specified Target")
    public Uni<MBeanMetrics> mbeanMetrics(@Source Target target) {
        var fTarget = Target.<Target>findById(target.id);
        return connectionManager.executeConnectedTaskUni(fTarget, JFRConnection::getMBeanMetrics);
    }

    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public static class Recordings {
        public @NonNull ActiveRecordings active;
        public @NonNull ArchivedRecordings archived;
    }

    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public static class ActiveRecordings {
        public @NonNull List<ActiveRecording> data;
        public @NonNull AggregateInfo aggregate;
    }

    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public static class ArchivedRecordings {
        public @NonNull List<ArchivedRecording> data;
        public @NonNull AggregateInfo aggregate;
    }

    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public static class AggregateInfo {
        public @NonNull @Description("The number of elements in this collection") long count;
        public @NonNull @Description(
                "The sum of sizes of elements in this collection, or 0 if not applicable") long
                size;

        private AggregateInfo(long count, long size) {
            this.count = count;
            this.size = size;
        }

        public static AggregateInfo empty() {
            return new AggregateInfo(0, 0);
        }

        public static AggregateInfo fromActive(List<ActiveRecording> recordings) {
            return new AggregateInfo(recordings.size(), 0);
        }

        public static AggregateInfo fromArchived(List<ArchivedRecording> recordings) {
            return new AggregateInfo(
                    recordings.size(),
                    recordings.stream().mapToLong(ArchivedRecording::size).sum());
        }
    }
}
