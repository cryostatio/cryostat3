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
package itest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.cryostat.util.HttpMimeType;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import itest.bases.StandardSelfTest;
import itest.util.ITestCleanupFailedException;
import itest.util.http.JvmIdWebRequest;
import itest.util.http.StoredCredential;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
public class CustomTargetsTest extends StandardSelfTest {

    private final ExecutorService worker = Executors.newCachedThreadPool();
    static final Map<String, String> NULL_RESULT = new HashMap<>();
    private String itestJvmId;
    private static StoredCredential storedCredential;

    String hostname;
    String jmxUrl;

    static {
        NULL_RESULT.put("result", null);
    }

    @BeforeEach
    void setup()
            throws InterruptedException,
                    ExecutionException,
                    TimeoutException,
                    UnknownHostException {
        hostname = InetAddress.getLocalHost().getHostName();
        jmxUrl = String.format("service:jmx:rmi:///jndi/rmi://%s:9091/jmxrmi", hostname);
        itestJvmId = JvmIdWebRequest.jvmIdRequest(getSelfReferenceConnectUrl());
    }

    @AfterAll
    static void cleanup() throws Exception {
        // Delete credentials to clean up
        CompletableFuture<JsonObject> deleteResponse = new CompletableFuture<>();
        webClient
                .delete("/api/v2.2/credentials/" + storedCredential.id)
                .send(
                        ar -> {
                            if (assertRequestStatus(ar, deleteResponse)) {
                                deleteResponse.complete(ar.result().bodyAsJsonObject());
                            }
                        });

        JsonObject expectedDeleteResponse =
                new JsonObject(
                        Map.of(
                                "meta",
                                Map.of("type", HttpMimeType.JSON.mime(), "status", "OK"),
                                "data",
                                NULL_RESULT));
        try {
            MatcherAssert.assertThat(
                    deleteResponse.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                    Matchers.equalTo(expectedDeleteResponse));
        } catch (Exception e) {
            logger.error(
                    new ITestCleanupFailedException(
                            String.format(
                                    "Failed to clean up credential with ID %d",
                                    storedCredential.id),
                            e));
        }
    }

    @Test
    @Order(1)
    void shouldBeAbleToTestTargetConnection()
            throws InterruptedException, ExecutionException, TimeoutException {
        MultiMap form = MultiMap.caseInsensitiveMultiMap();
        form.add("connectUrl", "localhost:0");
        form.add("alias", "self");

        HttpResponse<Buffer> response =
                webClient
                        .extensions()
                        .post("/api/v2/targets?dryrun=true", true, form, REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(response.statusCode(), Matchers.equalTo(202));
        JsonObject body = response.bodyAsJsonObject().getJsonObject("data").getJsonObject("result");
        MatcherAssert.assertThat(body.getString("connectUrl"), Matchers.equalTo("localhost:0"));
        MatcherAssert.assertThat(body.getString("alias"), Matchers.equalTo("self"));
    }

    @Test
    @Order(2)
    void targetShouldNotAppearInListing() throws InterruptedException, ExecutionException {
        CompletableFuture<JsonArray> response = new CompletableFuture<>();
        webClient
                .get("/api/v1/targets")
                .send(
                        ar -> {
                            assertRequestStatus(ar, response);
                            response.complete(ar.result().bodyAsJsonArray());
                        });
        JsonArray body = response.get();
        MatcherAssert.assertThat(body, Matchers.notNullValue());
        MatcherAssert.assertThat(body.size(), Matchers.equalTo(1));

        JsonObject selfJdp =
                new JsonObject(
                        Map.of(
                                "jvmId",
                                itestJvmId,
                                "alias",
                                "io.cryostat.Cryostat",
                                "connectUrl",
                                "service:jmx:rmi:///jndi/rmi://cryostat-itests:9091/jmxrmi",
                                "labels",
                                Map.of(),
                                "annotations",
                                Map.of(
                                        "cryostat",
                                        Map.of(
                                                "REALM",
                                                "JDP",
                                                "HOST",
                                                "cryostat-itests",
                                                "PORT",
                                                "9091",
                                                "JAVA_MAIN",
                                                "io.cryostat.Cryostat"),
                                        "platform",
                                        Map.of())));
        MatcherAssert.assertThat(body, Matchers.contains(selfJdp));
    }

    @Test
    @Order(3)
    void shouldBeAbleToDefineTarget()
            throws TimeoutException, ExecutionException, InterruptedException {
        MultiMap form = MultiMap.caseInsensitiveMultiMap();
        form.add("connectUrl", "localhost:0");
        form.add("alias", "self");
        form.add("username", "username");
        form.add("password", "password");

        CountDownLatch latch = new CountDownLatch(3);

        Future<JsonObject> resultFuture1 =
                worker.submit(
                        () -> {
                            try {
                                return expectNotification(
                                                "CredentialsStored",
                                                REQUEST_TIMEOUT_SECONDS,
                                                TimeUnit.SECONDS)
                                        .get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                latch.countDown();
                            }
                        });

        Future<JsonObject> resultFuture2 =
                worker.submit(
                        () -> {
                            try {
                                return expectNotification(
                                                "TargetJvmDiscovery",
                                                REQUEST_TIMEOUT_SECONDS,
                                                TimeUnit.SECONDS)
                                        .get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                latch.countDown();
                            }
                        });

        Thread.sleep(5000); // Sleep to setup notification listening before query resolves

        CompletableFuture<JsonObject> response = new CompletableFuture<>();
        webClient
                .post("/api/v2/targets?storeCredentials=true")
                .sendForm(
                        form,
                        ar -> {
                            assertRequestStatus(ar, response);
                            response.complete(ar.result().bodyAsJsonObject());
                            latch.countDown();
                        });
        latch.await(30, TimeUnit.SECONDS);

        JsonObject body = response.get().getJsonObject("data").getJsonObject("result");
        MatcherAssert.assertThat(body.getString("connectUrl"), Matchers.equalTo("localhost:0"));
        MatcherAssert.assertThat(body.getString("alias"), Matchers.equalTo("self"));

        JsonObject result1 = resultFuture1.get();

        JsonObject message = result1.getJsonObject("message");

        storedCredential =
                new StoredCredential(
                        message.getInteger("id"),
                        message.getString("matchExpression"),
                        message.getInteger("numMatchingTargets"));

        MatcherAssert.assertThat(storedCredential.id, Matchers.any(Integer.class));
        MatcherAssert.assertThat(
                storedCredential.matchExpression,
                Matchers.equalTo("target.connectUrl == \"localhost:0\""));
        MatcherAssert.assertThat(
                storedCredential.numMatchingTargets, Matchers.equalTo(Integer.valueOf(1)));

        JsonObject result2 = resultFuture2.get();
        JsonObject event = result2.getJsonObject("message").getJsonObject("event");
        MatcherAssert.assertThat(event.getString("kind"), Matchers.equalTo("FOUND"));
        MatcherAssert.assertThat(
                event.getJsonObject("serviceRef").getString("connectUrl"),
                Matchers.equalTo("localhost:0"));
        MatcherAssert.assertThat(
                event.getJsonObject("serviceRef").getString("alias"), Matchers.equalTo("self"));
    }

    @Test
    @Order(4)
    void targetShouldAppearInListing()
            throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<JsonArray> response = new CompletableFuture<>();
        webClient
                .get("/api/v1/targets")
                .send(
                        ar -> {
                            assertRequestStatus(ar, response);
                            response.complete(ar.result().bodyAsJsonArray());
                        });
        JsonArray body = response.get();
        MatcherAssert.assertThat(body, Matchers.notNullValue());
        MatcherAssert.assertThat(body.size(), Matchers.equalTo(2));

        JsonObject selfJdp =
                new JsonObject(
                        Map.of(
                                "jvmId",
                                itestJvmId,
                                "alias",
                                "io.cryostat.Cryostat",
                                "connectUrl",
                                "service:jmx:rmi:///jndi/rmi://cryostat-itests:9091/jmxrmi",
                                "labels",
                                Map.of(),
                                "annotations",
                                Map.of(
                                        "cryostat",
                                        Map.of(
                                                "REALM",
                                                "JDP",
                                                "HOST",
                                                "cryostat-itests",
                                                "PORT",
                                                "9091",
                                                "JAVA_MAIN",
                                                "io.cryostat.Cryostat"),
                                        "platform",
                                        Map.of())));
        JsonObject selfCustom =
                new JsonObject(
                        Map.of(
                                "jvmId",
                                itestJvmId,
                                "alias",
                                "self",
                                "connectUrl",
                                "localhost:0",
                                "labels",
                                Map.of(),
                                "annotations",
                                Map.of(
                                        "cryostat",
                                        Map.of("REALM", "Custom Targets"),
                                        "platform",
                                        Map.of())));
        MatcherAssert.assertThat(body, Matchers.containsInAnyOrder(selfJdp, selfCustom));
    }

    @Test
    @Order(5)
    void shouldBeAbleToDeleteTarget()
            throws TimeoutException, ExecutionException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);

        worker.submit(
                () -> {
                    try {
                        expectNotification(
                                        "TargetJvmDiscovery",
                                        REQUEST_TIMEOUT_SECONDS,
                                        TimeUnit.SECONDS)
                                .thenAcceptAsync(
                                        notification -> {
                                            JsonObject event =
                                                    notification
                                                            .getJsonObject("message")
                                                            .getJsonObject("event");
                                            MatcherAssert.assertThat(
                                                    event.getString("kind"),
                                                    Matchers.equalTo("LOST"));
                                            MatcherAssert.assertThat(
                                                    event.getJsonObject("serviceRef")
                                                            .getString("connectUrl"),
                                                    Matchers.equalTo("localhost:0"));
                                            MatcherAssert.assertThat(
                                                    event.getJsonObject("serviceRef")
                                                            .getString("alias"),
                                                    Matchers.equalTo("self"));
                                            latch.countDown();
                                        })
                                .get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        CompletableFuture<JsonObject> response = new CompletableFuture<>();
        webClient
                .delete("/api/v2/targets/localhost:0")
                .send(
                        ar -> {
                            assertRequestStatus(ar, response);
                            response.complete(null);
                            latch.countDown();
                        });

        latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    @Order(6)
    void targetShouldNoLongerAppearInListing() throws ExecutionException, InterruptedException {
        CompletableFuture<JsonArray> response = new CompletableFuture<>();
        webClient
                .get("/api/v1/targets")
                .send(
                        ar -> {
                            assertRequestStatus(ar, response);
                            response.complete(ar.result().bodyAsJsonArray());
                        });
        JsonArray body = response.get();
        MatcherAssert.assertThat(body, Matchers.notNullValue());
        MatcherAssert.assertThat(body.size(), Matchers.equalTo(1));

        JsonObject selfJdp =
                new JsonObject(
                        Map.of(
                                "jvmId",
                                itestJvmId,
                                "alias",
                                "io.cryostat.Cryostat",
                                "connectUrl",
                                "service:jmx:rmi:///jndi/rmi://cryostat-itests:9091/jmxrmi",
                                "labels",
                                Map.of(),
                                "annotations",
                                Map.of(
                                        "cryostat",
                                        Map.of(
                                                "REALM",
                                                "JDP",
                                                "HOST",
                                                "cryostat-itests",
                                                "PORT",
                                                "9091",
                                                "JAVA_MAIN",
                                                "io.cryostat.Cryostat"),
                                        "platform",
                                        Map.of())));
        MatcherAssert.assertThat(body, Matchers.contains(selfJdp));
    }
}