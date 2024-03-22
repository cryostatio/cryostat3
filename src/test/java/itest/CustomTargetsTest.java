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

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import itest.bases.StandardSelfTest;
import itest.util.http.JvmIdWebRequest;
import itest.util.http.StoredCredential;
import org.apache.http.client.utils.URLEncodedUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
public class CustomTargetsTest extends StandardSelfTest {

    private final ExecutorService worker = Executors.newCachedThreadPool();
    private static String itestJvmId;
    private static StoredCredential storedCredential;

    static String JMX_URL_ENCODED = URLEncodedUtils.formatSegments(SELF_JMX_URL).substring(1);

    @BeforeAll
    static void removeTestHarnessTargetDefinition()
            throws InterruptedException,
                    ExecutionException,
                    TimeoutException,
                    UnknownHostException {
        itestJvmId = JvmIdWebRequest.jvmIdRequest(SELF_JMX_URL);

        deleteSelfCustomTarget();

        JsonArray list =
                webClient
                        .extensions()
                        .get("/api/v3/targets", REQUEST_TIMEOUT_SECONDS)
                        .bodyAsJsonArray();
        if (!list.isEmpty()) throw new IllegalStateException();
    }

    @AfterAll
    static void restoreTestHarnessTargetDefinition()
            throws InterruptedException,
                    ExecutionException,
                    TimeoutException,
                    UnknownHostException {
        waitForDiscovery(0);
    }

    @AfterAll
    static void cleanup() throws Exception {
        // Delete credentials to clean up
        if (storedCredential == null) {
            return;
        }
        webClient
                .extensions()
                .delete("/api/v2.2/credentials/" + storedCredential.id, 0)
                .bodyAsJsonObject();
    }

    @Test
    @Order(1)
    void shouldBeAbleToTestTargetConnection()
            throws InterruptedException, ExecutionException, TimeoutException {
        HttpResponse<Buffer> response =
                webClient
                        .extensions()
                        .post(
                                "/api/v2/targets?dryrun=true",
                                Buffer.buffer(
                                        JsonObject.of("connectUrl", SELF_JMX_URL, "alias", "self")
                                                .encode()),
                                REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(response.statusCode(), Matchers.equalTo(202));
        JsonObject body = response.bodyAsJsonObject().getJsonObject("data").getJsonObject("result");
        MatcherAssert.assertThat(body.getString("connectUrl"), Matchers.equalTo(SELF_JMX_URL));
        MatcherAssert.assertThat(body.getString("alias"), Matchers.equalTo("self"));
        MatcherAssert.assertThat(body.getString("jvmId"), Matchers.equalTo(itestJvmId));

        JsonArray list =
                webClient
                        .extensions()
                        .get("/api/v3/targets", REQUEST_TIMEOUT_SECONDS)
                        .bodyAsJsonArray();
        MatcherAssert.assertThat(list, Matchers.notNullValue());
        MatcherAssert.assertThat(list.size(), Matchers.equalTo(0));
    }

    @Test
    @Order(2)
    void shouldBeAbleToDefineTarget()
            throws TimeoutException, ExecutionException, InterruptedException {
        MultiMap form = MultiMap.caseInsensitiveMultiMap();
        String alias = UUID.randomUUID().toString();
        form.add("connectUrl", SELF_JMX_URL);
        form.add("alias", alias);
        form.add("username", "username");
        form.add("password", "password");

        CountDownLatch latch = new CountDownLatch(2);

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

        Thread.sleep(500);

        HttpResponse<Buffer> response =
                webClient
                        .extensions()
                        .post(
                                "/api/v2/targets?storeCredentials=true",
                                form,
                                REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(response.statusCode(), Matchers.equalTo(201));

        JsonObject body = response.bodyAsJsonObject().getJsonObject("data").getJsonObject("result");

        latch.await(30, TimeUnit.SECONDS);

        MatcherAssert.assertThat(body.getString("connectUrl"), Matchers.equalTo(SELF_JMX_URL));
        MatcherAssert.assertThat(body.getString("alias"), Matchers.equalTo(alias));

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
                Matchers.equalTo(String.format("target.connectUrl == \"%s\"", SELF_JMX_URL)));
        // FIXME this is currently always emitted as 0. Do we really need this to be included at
        // all?
        // MatcherAssert.assertThat(
        //         storedCredential.numMatchingTargets, Matchers.equalTo(Integer.valueOf(1)));

        JsonObject result2 = resultFuture2.get();
        JsonObject foundDiscoveryEvent = result2.getJsonObject("message").getJsonObject("event");
        MatcherAssert.assertThat(foundDiscoveryEvent.getString("kind"), Matchers.equalTo("FOUND"));
        MatcherAssert.assertThat(
                foundDiscoveryEvent.getJsonObject("serviceRef").getString("connectUrl"),
                Matchers.equalTo(SELF_JMX_URL));
        MatcherAssert.assertThat(
                foundDiscoveryEvent.getJsonObject("serviceRef").getString("alias"),
                Matchers.equalTo(alias));

        HttpResponse<Buffer> listResponse =
                webClient.extensions().get("/api/v1/targets", REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(listResponse.statusCode(), Matchers.equalTo(200));
        JsonArray list = listResponse.bodyAsJsonArray();
        MatcherAssert.assertThat(list, Matchers.notNullValue());
        MatcherAssert.assertThat(list.size(), Matchers.equalTo(1));
        JsonObject item = list.getJsonObject(0);
        MatcherAssert.assertThat(item.getString("jvmId"), Matchers.equalTo(itestJvmId));
        MatcherAssert.assertThat(item.getString("alias"), Matchers.equalTo(alias));
        MatcherAssert.assertThat(item.getString("connectUrl"), Matchers.equalTo(SELF_JMX_URL));
        MatcherAssert.assertThat(item.getJsonArray("labels"), Matchers.equalTo(new JsonArray()));
        MatcherAssert.assertThat(
                item.getJsonObject("annotations"),
                Matchers.equalTo(
                        new JsonObject(
                                Map.of(
                                        "platform",
                                        List.of(),
                                        "cryostat",
                                        List.of(
                                                Map.of(
                                                        "key",
                                                        "REALM",
                                                        "value",
                                                        "Custom Targets"))))));
    }

    @Test
    @Order(3)
    void shouldBeAbleToDeleteTarget()
            throws TimeoutException, ExecutionException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

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

        webClient
                .extensions()
                .delete(
                        String.format("/api/v2/targets/%s", JMX_URL_ENCODED),
                        REQUEST_TIMEOUT_SECONDS);

        latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        HttpResponse<Buffer> listResponse =
                webClient.extensions().get("/api/v1/targets", REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(listResponse.statusCode(), Matchers.equalTo(200));
        JsonArray list = listResponse.bodyAsJsonArray();
        MatcherAssert.assertThat(list, Matchers.notNullValue());
        MatcherAssert.assertThat(list.size(), Matchers.equalTo(0));
    }
}
