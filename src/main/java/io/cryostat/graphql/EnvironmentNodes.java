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

import io.cryostat.discovery.DiscoveryNode;
import io.cryostat.graphql.RootNode.DiscoveryNodeFilter;

import io.smallrye.graphql.api.Nullable;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Query;

@GraphQLApi
public class EnvironmentNodes {

    @Query("environmentNodes")
    @Description("Get all environment nodes in the discovery tree with optional filtering")
    public List<DiscoveryNode> environmentNodes(@Nullable DiscoveryNodeFilter filter) {
        return RootNode.recurseChildren(DiscoveryNode.getUniverse(), node -> node.target == null)
                .stream()
                .filter(filter)
                .toList();
    }
}
