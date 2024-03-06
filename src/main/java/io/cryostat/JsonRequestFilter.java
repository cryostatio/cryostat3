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
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

@Provider
public class JsonRequestFilter implements ContainerRequestFilter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        if (requestContext.getMediaType() != null
                && requestContext.getMediaType().isCompatible(MediaType.APPLICATION_JSON_TYPE)) {
            byte[] jsonData = requestContext.getEntityStream().readAllBytes();
            String json = new String(jsonData, StandardCharsets.UTF_8);
            JsonNode rootNode = objectMapper.readTree(json);

            if (containsIdField(rootNode)) {
                requestContext.abortWith(
                        Response.status(Response.Status.BAD_REQUEST)
                                .entity("ID field cannot be specified in the request body.")
                                .build());
                return;
            }

            requestContext.setEntityStream(new ByteArrayInputStream(json.getBytes()));
        }
    }

    private boolean containsIdField(JsonNode node) {
        if (node.has("id")) {
            return true;
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
