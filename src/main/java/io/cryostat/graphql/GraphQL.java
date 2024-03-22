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

import java.net.URI;

import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.apache.hc.core5.net.URIBuilder;
import org.jboss.resteasy.reactive.RestResponse;

@Path("")
public class GraphQL {

    @GET
    @Path("/api/v2.2/graphql")
    @RolesAllowed("write")
    public Response redirectGet(UriInfo info) throws Exception {
        var uriBuilder = new URIBuilder();
        info.getQueryParameters()
                .entrySet()
                .forEach(
                        entry -> {
                            if (entry.getValue().size() != 1) {
                                return;
                            }
                            uriBuilder.addParameter(entry.getKey(), entry.getValue().get(0));
                        });
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(
                        URI.create(
                                String.format(
                                        "/api/v3/graphql?%s",
                                        String.join("&", uriBuilder.build().getRawQuery()))))
                .build();
    }

    @POST
    @Path("/api/v2.2/graphql")
    @RolesAllowed("write")
    public Response redirectPost() throws Exception {
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(URI.create("/api/v3/graphql"))
                .build();
    }
}
