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
package io.cryostat.discovery;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.cryostat.ConfigProperties;
import io.cryostat.V2Response;
import io.cryostat.credentials.Credential;
import io.cryostat.expressions.MatchExpression;
import io.cryostat.targets.JvmIdException;
import io.cryostat.targets.Target;
import io.cryostat.targets.Target.Annotations;
import io.cryostat.targets.TargetConnectionManager;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Blocking;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.annotation.security.RolesAllowed;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hibernate.exception.ConstraintViolationException;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;
import org.jboss.resteasy.reactive.RestResponse;

@ApplicationScoped
@Path("")
public class CustomDiscovery {

    public static final Pattern HOST_PORT_PAIR_PATTERN =
            Pattern.compile("^([^:\\s]+)(?::(\\d{1,5}))$");
    private static final String REALM = "Custom Targets";

    @Inject Logger logger;
    @Inject EventBus bus;
    @Inject TargetConnectionManager connectionManager;

    @ConfigProperty(name = ConfigProperties.CONNECTIONS_FAILED_TIMEOUT)
    Duration timeout;

    @Transactional
    void onStart(@Observes StartupEvent evt) {
        DiscoveryNode universe = DiscoveryNode.getUniverse();
        if (DiscoveryNode.getRealm(REALM).isEmpty()) {
            DiscoveryPlugin plugin = new DiscoveryPlugin();
            DiscoveryNode node = DiscoveryNode.environment(REALM, DiscoveryNode.REALM);
            plugin.realm = node;
            plugin.builtin = true;
            universe.children.add(node);
            plugin.persist();
            universe.persist();
        }
    }

    @Transactional(rollbackOn = {JvmIdException.class})
    @POST
    @Path("/api/v2/targets")
    @Consumes(MediaType.APPLICATION_JSON)
    @RolesAllowed("write")
    public Response create(
            Target target, @RestQuery boolean dryrun, @RestQuery boolean storeCredentials) {
        // TODO handle credentials embedded in JSON body
        return doV2Create(target, Optional.empty(), dryrun, storeCredentials);
    }

    @Transactional
    @POST
    @Path("/api/v2/targets")
    @Consumes({MediaType.MULTIPART_FORM_DATA, MediaType.APPLICATION_FORM_URLENCODED})
    @RolesAllowed("write")
    public Response create(
            @RestForm URI connectUrl,
            @RestForm String alias,
            @RestForm String username,
            @RestForm String password,
            @RestQuery boolean dryrun,
            @RestQuery boolean storeCredentials) {
        var target = new Target();
        target.connectUrl = connectUrl;
        target.alias = alias;

        Credential credential = null;
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            credential = new Credential();
            credential.matchExpression =
                    new MatchExpression(
                            String.format("target.connectUrl == \"%s\"", connectUrl.toString()));
            credential.username = username;
            credential.password = password;
        }

        return doV2Create(target, Optional.ofNullable(credential), dryrun, storeCredentials);
    }

    @Transactional
    @Blocking
    Response doV2Create(
            Target target,
            Optional<Credential> credential,
            boolean dryrun,
            boolean storeCredentials) {
        try {
            target.connectUrl = sanitizeConnectUrl(target.connectUrl.toString());

            try {
                target.jvmId =
                        connectionManager
                                .executeDirect(
                                        target,
                                        credential,
                                        conn -> conn.getJvmIdentifier().getHash())
                                .await()
                                .atMost(timeout);
            } catch (Exception e) {
                logger.error("Target connection failed", e);
                return Response.status(Response.Status.BAD_REQUEST.getStatusCode())
                        .entity(V2Response.json(Response.Status.BAD_REQUEST, e))
                        .build();
            }

            if (dryrun) {
                return Response.accepted()
                        .entity(V2Response.json(Response.Status.ACCEPTED, target))
                        .build();
            }

            target.persist();
            credential.ifPresent(c -> c.persist());

            target.activeRecordings = new ArrayList<>();
            target.labels = Map.of();
            target.annotations = new Annotations();
            target.annotations.cryostat().putAll(Map.of("REALM", REALM));

            DiscoveryNode node = DiscoveryNode.target(target);
            target.discoveryNode = node;
            DiscoveryNode realm = DiscoveryNode.getRealm(REALM).orElseThrow();

            realm.children.add(node);
            target.persist();
            node.persist();
            realm.persist();

            return Response.created(URI.create("/api/v3/targets/" + target.id))
                    .entity(V2Response.json(Response.Status.CREATED, target))
                    .build();
        } catch (Exception e) {
            if (ExceptionUtils.indexOfType(e, ConstraintViolationException.class) >= 0) {
                logger.warn("Invalid target definition", e);
                return Response.status(Response.Status.BAD_REQUEST.getStatusCode())
                        .entity(V2Response.json(Response.Status.BAD_REQUEST, e))
                        .build();
            }
            logger.error("Unknown error", e);
            return Response.serverError()
                    .entity(V2Response.json(Response.Status.INTERNAL_SERVER_ERROR, e))
                    .build();
        }
    }

    @Transactional
    @DELETE
    @Path("/api/v2/targets/{connectUrl}")
    @RolesAllowed("write")
    public Response delete(@RestPath URI connectUrl) throws URISyntaxException {
        Target target = Target.getTargetByConnectUrl(connectUrl);
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(URI.create(String.format("/api/v3/targets/%d", target.id)))
                .build();
    }

    @Transactional
    @DELETE
    @Path("/api/v3/targets/{id}")
    @RolesAllowed("write")
    public Response delete(@RestPath long id) throws URISyntaxException {
        Target target = Target.find("id", id).singleResult();
        DiscoveryNode realm = DiscoveryNode.getRealm(REALM).orElseThrow();
        realm.children.remove(target.discoveryNode);
        realm.persist();
        target.delete();
        return Response.noContent().build();
    }

    private URI sanitizeConnectUrl(String in) throws URISyntaxException, MalformedURLException {
        URI out;

        Matcher m = HOST_PORT_PAIR_PATTERN.matcher(in);
        if (m.find()) {
            String host = m.group(1);
            String port = m.group(2);
            out =
                    URI.create(
                            String.format(
                                    "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi",
                                    host, Integer.valueOf(port)));
        } else {
            out = new URI(in);
        }

        return out;
    }
}
