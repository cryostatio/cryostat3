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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import io.cryostat.discovery.DiscoveryNode;
import io.cryostat.graphql.matchers.LabelSelectorMatcher;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.smallrye.graphql.api.Nullable;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Query;
import org.eclipse.microprofile.graphql.Source;

@GraphQLApi
public class RootNode {

    @Query("rootNode")
    @Description("Get the root target discovery node")
    public DiscoveryNode getRootNode() {
        return DiscoveryNode.getUniverse();
    }

    @Description(
            "Get target nodes that are descendants of this node. That is, get the set of leaf nodes"
                    + " from anywhere below this node's subtree.")
    public List<DiscoveryNode> descendantTargets(
            @Source DiscoveryNode discoveryNode, DiscoveryNodeFilter filter) {
        // TODO do this filtering at the database query level as much as possible. As is, this will
        // load the entire discovery tree out of the database, then perform the filtering at the
        // application level.
        return recurseChildren(discoveryNode, n -> n.target != null).stream()
                .filter(n -> filter == null ? true : filter.test(n))
                .toList();
    }

    static Set<DiscoveryNode> recurseChildren(
            DiscoveryNode node, Predicate<DiscoveryNode> predicate) {
        Set<DiscoveryNode> result = new HashSet<>();
        if (predicate.test(node)) {
            result.add(node);
        }
        if (node.children != null) {
            node.children.forEach(c -> result.addAll(recurseChildren(c, predicate)));
        }
        return result;
    }

    @SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
    public static class DiscoveryNodeFilter implements Predicate<DiscoveryNode> {
        public @Nullable Long id;
        public @Nullable List<Long> ids;
        public @Nullable String name;
        public @Nullable List<String> names;
        public @Nullable List<String> nodeTypes;
        public @Nullable List<String> labels;
        public @Nullable List<String> annotations;

        @Override
        public boolean test(DiscoveryNode t) {
            Predicate<DiscoveryNode> matchesId = n -> id == null || id.equals(n.id);
            Predicate<DiscoveryNode> matchesIds = n -> ids == null || ids.contains(n.id);
            Predicate<DiscoveryNode> matchesName = n -> name == null || name.equals(n.name);
            Predicate<DiscoveryNode> matchesNames = n -> names == null || names.contains(n.name);
            Predicate<DiscoveryNode> matchesNodeTypes =
                    n -> nodeTypes == null || nodeTypes.contains(n.nodeType);
            Predicate<DiscoveryNode> matchesLabels =
                    n ->
                            labels == null
                                    || labels.stream()
                                            .allMatch(
                                                    label ->
                                                            LabelSelectorMatcher.parse(label)
                                                                    .test(n.labels));
            Predicate<DiscoveryNode> matchesAnnotations =
                    n ->
                            annotations == null
                                    || annotations.stream()
                                            .allMatch(
                                                    annotation ->
                                                            LabelSelectorMatcher.parse(annotation)
                                                                    .test(
                                                                            n.target.annotations
                                                                                    .merged()));

            return List.of(
                            matchesId,
                            matchesIds,
                            matchesName,
                            matchesNames,
                            matchesNodeTypes,
                            matchesLabels,
                            matchesAnnotations)
                    .stream()
                    .reduce(x -> true, Predicate::and)
                    .test(t);
        }
    }
}
