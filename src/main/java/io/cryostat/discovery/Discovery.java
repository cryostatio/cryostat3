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

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import io.cryostat.credentials.Credential;
import io.cryostat.discovery.DiscoveryPlugin.PluginCallback;
import io.cryostat.targets.TargetConnectionManager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jwt.proc.BadJWTException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Blocking;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.annotation.security.PermitAll;
import jakarta.annotation.security.RolesAllowed;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.RestResponse.ResponseBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.impl.matchers.GroupMatcher;

@Path("")
public class Discovery {

    static final String X_FORWARDED_FOR = "X-Forwarded-For";

    private static final String JOB_PERIODIC = "periodic";
    private static final String JOB_STARTUP = "startup";
    private static final String PLUGIN_ID_MAP_KEY = "pluginId";
    private static final String REFRESH_MAP_KEY = "refresh";

    @ConfigProperty(name = "cryostat.discovery.plugins.ping-period")
    Duration discoveryPingPeriod;

    @Inject Logger logger;
    @Inject ObjectMapper mapper;
    @Inject EventBus bus;
    @Inject TargetConnectionManager connectionManager;
    @Inject DiscoveryJwtFactory jwtFactory;
    @Inject DiscoveryJwtValidator jwtValidator;
    @Inject Scheduler scheduler;

    @Transactional
    void onStart(@Observes StartupEvent evt) {
        // ensure lazily initialized entries are created
        DiscoveryNode.getUniverse();

        DiscoveryPlugin.<DiscoveryPlugin>findAll().list().stream()
                .filter(p -> !p.builtin)
                .forEach(
                        plugin -> {
                            var dataMap = new JobDataMap();
                            dataMap.put(PLUGIN_ID_MAP_KEY, plugin.id);
                            dataMap.put(REFRESH_MAP_KEY, true);
                            JobDetail jobDetail =
                                    JobBuilder.newJob(RefreshPluginJob.class)
                                            .withIdentity(plugin.id.toString(), JOB_STARTUP)
                                            .usingJobData(dataMap)
                                            .build();
                            var trigger =
                                    TriggerBuilder.newTrigger()
                                            .usingJobData(jobDetail.getJobDataMap())
                                            .startNow()
                                            .withSchedule(
                                                    SimpleScheduleBuilder.simpleSchedule()
                                                            .withRepeatCount(0))
                                            .build();
                            try {
                                scheduler.scheduleJob(jobDetail, trigger);
                            } catch (SchedulerException e) {
                                logger.warn("Failed to schedule plugin prune job", e);
                            }
                        });
    }

    void onStop(@Observes ShutdownEvent evt) throws SchedulerException {
        scheduler.shutdown();
    }

    @GET
    @Path("/api/v2.1/discovery")
    @RolesAllowed("read")
    public Response getv21() {
        return Response.status(RestResponse.Status.PERMANENT_REDIRECT)
                .location(URI.create("/api/v3/discovery"))
                .build();
    }

    @GET
    @Path("/api/v3/discovery")
    @RolesAllowed("read")
    public DiscoveryNode get() {
        return DiscoveryNode.getUniverse();
    }

    @GET
    @Path("/api/v2.2/discovery/{id}")
    @RolesAllowed("read")
    public RestResponse<Void> checkRegistration(
            @Context RoutingContext ctx, @RestPath UUID id, @RestQuery String token)
            throws SocketException,
                    UnknownHostException,
                    MalformedURLException,
                    ParseException,
                    JOSEException,
                    URISyntaxException {
        DiscoveryPlugin plugin = DiscoveryPlugin.find("id", id).singleResult();
        jwtValidator.validateJwt(ctx, plugin, token, true);
        return ResponseBuilder.<Void>ok().build();
    }

    @Transactional
    @POST
    @Path("/api/v2.2/discovery")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @RolesAllowed("write")
    public Response register(@Context RoutingContext ctx, JsonObject body)
            throws URISyntaxException,
                    JOSEException,
                    UnknownHostException,
                    SocketException,
                    ParseException,
                    BadJWTException,
                    SchedulerException {
        String pluginId = body.getString("id");
        String priorToken = body.getString("token");
        String realmName = body.getString("realm");
        URI callbackUri = new URI(body.getString("callback"));

        // TODO apply URI range validation to the remote address
        InetAddress remoteAddress = getRemoteAddress(ctx);

        URI location;
        DiscoveryPlugin plugin;
        if (StringUtils.isNotBlank(pluginId) && StringUtils.isNotBlank(priorToken)) {
            // refresh the JWT for existing registration
            plugin =
                    DiscoveryPlugin.<DiscoveryPlugin>find("id", UUID.fromString(pluginId))
                            .singleResult();
            if (!Objects.equals(plugin.realm.name, realmName)) {
                throw new ForbiddenException();
            }
            if (!Objects.equals(plugin.callback, callbackUri)) {
                throw new BadRequestException();
            }
            location = jwtFactory.getPluginLocation(plugin);
            jwtFactory.parseDiscoveryPluginJwt(plugin, priorToken, location, remoteAddress, false);
        } else {
            // new plugin registration
            plugin = new DiscoveryPlugin();
            plugin.callback = callbackUri;
            plugin.realm =
                    DiscoveryNode.environment(
                            requireNonBlank(realmName, "realm"), DiscoveryNode.REALM);
            plugin.builtin = false;
            plugin.persist();

            DiscoveryNode.getUniverse().children.add(plugin.realm);

            location = jwtFactory.getPluginLocation(plugin);

            var dataMap = new JobDataMap();
            dataMap.put(PLUGIN_ID_MAP_KEY, plugin.id);
            dataMap.put(REFRESH_MAP_KEY, true);
            JobDetail jobDetail =
                    JobBuilder.newJob(RefreshPluginJob.class)
                            .withIdentity(plugin.id.toString(), JOB_PERIODIC)
                            .usingJobData(dataMap)
                            .build();
            var trigger =
                    TriggerBuilder.newTrigger()
                            .usingJobData(jobDetail.getJobDataMap())
                            .startAt(Date.from(Instant.now().plus(discoveryPingPeriod)))
                            .withSchedule(
                                    SimpleScheduleBuilder.simpleSchedule()
                                            .repeatForever()
                                            .withIntervalInSeconds(
                                                    (int) discoveryPingPeriod.toSeconds()))
                            .build();
            scheduler.scheduleJob(jobDetail, trigger);
        }

        String token = jwtFactory.createDiscoveryPluginJwt(plugin, remoteAddress, location);

        // TODO implement more generic env map passing by some platform detection strategy or
        // generalized config properties
        var envMap = new HashMap<String, String>();
        String insightsProxy = System.getenv("INSIGHTS_PROXY");
        if (StringUtils.isNotBlank(insightsProxy)) {
            envMap.put("INSIGHTS_SVC", "INSIGHTS_PROXY");
        }
        return Response.created(location)
                .entity(
                        Map.of(
                                "meta",
                                        Map.of(
                                                "mimeType", "JSON",
                                                "status", "OK"),
                                "data",
                                        Map.of(
                                                "result",
                                                Map.of(
                                                        "id",
                                                        plugin.id.toString(),
                                                        "token",
                                                        token,
                                                        "env",
                                                        envMap))))
                .build();
    }

    @Transactional
    @POST
    @Path("/api/v2.2/discovery/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @PermitAll
    public Map<String, Map<String, String>> publish(
            @Context RoutingContext ctx,
            @RestPath UUID id,
            @RestQuery String token,
            List<DiscoveryNode> body)
            throws SocketException,
                    UnknownHostException,
                    MalformedURLException,
                    ParseException,
                    JOSEException,
                    URISyntaxException {
        DiscoveryPlugin plugin = DiscoveryPlugin.find("id", id).singleResult();
        jwtValidator.validateJwt(ctx, plugin, token, true);
        plugin.realm.children.clear();
        plugin.persist();
        plugin.realm.children.addAll(body);
        body.forEach(
                b -> {
                    if (b.target != null) {
                        b.target.discoveryNode = b;
                    }
                    b.persist();
                });
        plugin.persist();

        return Map.of(
                "meta",
                        Map.of(
                                "mimeType", "JSON",
                                "status", "OK"),
                "data", Map.of("result", plugin.id.toString()));
    }

    @Transactional
    @DELETE
    @Path("/api/v2.2/discovery/{id}")
    @PermitAll
    public Map<String, Map<String, String>> deregister(
            @Context RoutingContext ctx, @RestPath UUID id, @RestQuery String token)
            throws SocketException,
                    UnknownHostException,
                    MalformedURLException,
                    ParseException,
                    JOSEException,
                    URISyntaxException,
                    SchedulerException {
        DiscoveryPlugin plugin = DiscoveryPlugin.find("id", id).singleResult();
        jwtValidator.validateJwt(ctx, plugin, token, false);
        if (plugin.builtin) {
            throw new ForbiddenException();
        }

        Set<JobKey> jobKeys = new HashSet<>();
        jobKeys.addAll(scheduler.getJobKeys(GroupMatcher.jobGroupEquals(JOB_PERIODIC)));
        jobKeys.addAll(scheduler.getJobKeys(GroupMatcher.jobGroupEquals(JOB_STARTUP)));
        for (var key : jobKeys) {
            scheduler.deleteJob(key);
        }

        plugin.realm.delete();
        plugin.delete();
        getStoredCredential(plugin).ifPresent(Credential::delete);
        DiscoveryNode.getUniverse().children.remove(plugin.realm);
        return Map.of(
                "meta",
                        Map.of(
                                "mimeType", "JSON",
                                "status", "OK"),
                "data", Map.of("result", plugin.id.toString()));
    }

    @GET
    @Path("/api/v3/discovery_plugins")
    @RolesAllowed("read")
    public Response getPlugins(@RestQuery String realm) throws JsonProcessingException {
        // TODO filter for the matching realm name within the DB query
        List<DiscoveryPlugin> plugins =
                DiscoveryPlugin.findAll().<DiscoveryPlugin>list().stream()
                        .filter(p -> StringUtils.isBlank(realm) || p.realm.name.equals(realm))
                        .toList();
        return Response.ok()
                .entity(
                        mapper.writerWithView(DiscoveryNode.Views.Flat.class)
                                .writeValueAsString(plugins))
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    @GET
    @Path("/api/v3/discovery_plugins/{id}")
    @RolesAllowed("read")
    public DiscoveryPlugin getPlugin(@RestPath UUID id) throws JsonProcessingException {
        return DiscoveryPlugin.find("id", id).singleResult();
    }

    Optional<Credential> getStoredCredential(DiscoveryPlugin plugin) {
        return new DiscoveryPlugin.PluginCallback.DiscoveryPluginAuthorizationHeaderFactory(plugin)
                .getCredential();
    }

    static class RefreshPluginJob implements Job {
        @Inject Logger logger;

        @Override
        @Transactional
        @Blocking
        @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NULL_VALUE")
        public void execute(JobExecutionContext context) throws JobExecutionException {
            DiscoveryPlugin plugin = null;
            try {
                boolean refresh = context.getMergedJobDataMap().getBoolean(REFRESH_MAP_KEY);
                plugin =
                        DiscoveryPlugin.find(
                                        "id", context.getMergedJobDataMap().get(PLUGIN_ID_MAP_KEY))
                                .singleResult();
                var cb = PluginCallback.create(plugin);
                if (refresh) {
                    cb.refresh();
                    logger.debugv(
                            "Refreshed discovery plugin: {0} @ {1}", plugin.realm, plugin.callback);
                } else {
                    cb.ping();
                    logger.debugv(
                            "Retained discovery plugin: {0} @ {1}", plugin.realm, plugin.callback);
                }
            } catch (Exception e) {
                if (plugin != null) {
                    logger.debugv(
                            e, "Pruned discovery plugin: {0} @ {1}", plugin.realm, plugin.callback);
                    plugin.realm.delete();
                    plugin.delete();
                    new DiscoveryPlugin.PluginCallback.DiscoveryPluginAuthorizationHeaderFactory(
                                    plugin)
                            .getCredential()
                            .ifPresent(Credential::delete);
                }
                throw new JobExecutionException(e);
            }
        }
    }

    static String requireNonBlank(String in, String name) {
        if (StringUtils.isBlank(in)) {
            throw new IllegalArgumentException(
                    String.format("Parameter \"%s\" may not be blank", name));
        }
        return in;
    }

    private InetAddress getRemoteAddress(RoutingContext ctx) {
        InetAddress addr = null;
        if (ctx.request() != null && ctx.request().remoteAddress() != null) {
            addr = jwtValidator.tryResolveAddress(addr, ctx.request().remoteAddress().host());
        }
        if (ctx.request() != null && ctx.request().headers() != null) {
            addr =
                    jwtValidator.tryResolveAddress(
                            addr, ctx.request().headers().get(X_FORWARDED_FOR));
        }
        return addr;
    }
}
