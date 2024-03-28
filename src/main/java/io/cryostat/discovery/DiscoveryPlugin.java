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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;

import io.cryostat.credentials.Credential;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
import jakarta.persistence.PrePersist;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.rest.client.ext.ClientHeadersFactory;
import org.hibernate.annotations.GenericGenerator;
import org.jboss.logging.Logger;

@Entity
@EntityListeners(DiscoveryPlugin.Listener.class)
public class DiscoveryPlugin extends PanacheEntityBase {

    @Id
    @Column(name = "id")
    @GeneratedValue(generator = "UUID")
    @GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
    @NotNull
    public UUID id;

    @OneToOne(
            optional = false,
            cascade = {CascadeType.ALL},
            orphanRemoval = true,
            fetch = FetchType.LAZY)
    @NotNull
    public DiscoveryNode realm;

    @Column(unique = true, updatable = false)
    @Convert(converter = UriConverter.class)
    public URI callback;

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public boolean builtin;

    @ApplicationScoped
    static class Listener {

        @Inject Logger logger;

        @PrePersist
        public void prePersist(DiscoveryPlugin plugin) {
            if (plugin.builtin) {
                return;
            }
            if (plugin.callback == null) {
                plugin.realm.delete();
                plugin.delete();
                throw new IllegalArgumentException();
            }
            try {
                PluginCallback.create(plugin).ping();
                logger.infov(
                        "Registered discovery plugin: {0} @ {1}",
                        plugin.realm.name, plugin.callback);
            } catch (URISyntaxException e) {
                plugin.realm.delete();
                plugin.delete();
                throw new IllegalArgumentException(e);
            } catch (Exception e) {
                plugin.realm.delete();
                plugin.delete();
                logger.error("Discovery Plugin ping failed", e);
                throw e;
            }
        }
    }

    @Path("")
    interface PluginCallback {

        final Logger logger = Logger.getLogger(PluginCallback.class);

        @GET
        public void ping();

        @POST
        public void refresh();

        public static PluginCallback create(DiscoveryPlugin plugin) throws URISyntaxException {
            if (StringUtils.isBlank(plugin.callback.getUserInfo())) {
                logger.warnv(
                        "Plugin with id:{0} realm:{1} callback:{2} did not supply userinfo",
                        plugin.id, plugin.realm, plugin.callback);
            }

            PluginCallback client =
                    QuarkusRestClientBuilder.newBuilder()
                            .baseUri(plugin.callback)
                            .clientHeadersFactory(
                                    new DiscoveryPluginAuthorizationHeaderFactory(plugin))
                            .build(PluginCallback.class);
            return client;
        }

        public static class DiscoveryPluginAuthorizationHeaderFactory
                implements ClientHeadersFactory {

            private final DiscoveryPlugin plugin;

            @SuppressFBWarnings("EI_EXPOSE_REP2")
            public DiscoveryPluginAuthorizationHeaderFactory(DiscoveryPlugin plugin) {
                this.plugin = plugin;
            }

            public Optional<Credential> getCredential() {
                String userInfo = plugin.callback.getUserInfo();
                if (StringUtils.isBlank(userInfo)) {
                    logger.error("No stored credentials specified");
                    return Optional.empty();
                }

                if (!userInfo.contains(":")) {
                    logger.errorv("Unexpected non-basic credential format, found: {0}", userInfo);
                    return Optional.empty();
                }

                String[] parts = userInfo.split(":");
                if (parts.length != 2) {
                    logger.errorv("Unexpected basic credential format, found: {0}", userInfo);
                    return Optional.empty();
                }

                if (!"storedcredentials".equals(parts[0])) {
                    logger.errorv(
                            "Unexpected credential format, expected \"storedcredentials\" but"
                                    + " found: {0}",
                            parts[0]);
                    return Optional.empty();
                }

                return Credential.find("id", Long.parseLong(parts[1])).singleResultOptional();
            }

            @Override
            public MultivaluedMap<String, String> update(
                    MultivaluedMap<String, String> incomingHeaders,
                    MultivaluedMap<String, String> clientOutgoingHeaders) {
                var result = new MultivaluedHashMap<String, String>();
                Optional<Credential> opt = getCredential();
                opt.ifPresent(
                        credential -> {
                            String basicAuth = credential.username + ":" + credential.password;
                            byte[] authBytes = basicAuth.getBytes(StandardCharsets.UTF_8);
                            String base64Auth = Base64.getEncoder().encodeToString(authBytes);
                            result.add(
                                    HttpHeaders.AUTHORIZATION,
                                    String.format("Basic %s", base64Auth));
                        });
                return result;
            }
        }
    }
}
