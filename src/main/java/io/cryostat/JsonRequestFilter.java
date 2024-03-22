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
package io.cryostat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

@Provider
public class JsonRequestFilter implements ContainerRequestFilter {

    static final Set<String> disallowedFields = Set.of("id");
    static final Set<String> allowedPaths =
            Set.of("/api/v2.2/discovery", "/api/v2.2/graphql", "/api/v3/graphql");

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (requestContext.getMediaType() != null
                && requestContext.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE)
                && (requestContext.getUriInfo() != null
                        && !allowedPaths.contains(requestContext.getUriInfo().getPath()))) {
            try (InputStream stream = requestContext.getEntityStream()) {
                JsonNode rootNode = objectMapper.readTree(stream);

                if (containsIdField(rootNode)) {
                    requestContext.abortWith(
                            Response.status(Response.Status.BAD_REQUEST)
                                    .entity("ID field cannot be specified in the request body.")
                                    .build());
                    return;
                }

                requestContext.setEntityStream(
                        new ByteArrayInputStream(
                                rootNode.toString().getBytes(StandardCharsets.UTF_8)));
            }
        }
    }

    private boolean containsIdField(JsonNode node) {
        for (String field : disallowedFields) {
            if (node.has(field)) {
                return true;
            }
        }
        if (node.isContainerNode()) {
            for (JsonNode child : node) {
                if (containsIdField(child)) {
                    return true;
                }
            }
        }
        return false;
    }
}
