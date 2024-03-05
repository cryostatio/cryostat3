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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.cryostat.discovery.DiscoveryNode;
import io.cryostat.util.HttpMimeType;

import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import itest.bases.StandardSelfTest;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@TestMethodOrder(OrderAnnotation.class)
// @Disabled("TODO not all GraphQL queries are implemented")
class GraphQLTest extends StandardSelfTest {

    private final ExecutorService worker = ForkJoinPool.commonPool();

    static final String TEST_RECORDING_NAME = "archivedRecording";

    @Test
    @Order(0)
    void testEnvironmentNodeListing() throws Exception {
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { environmentNodes(filter: { name: \"Custom Targets\" }) { name nodeType"
                        + " children { name nodeType } } }");

        System.out.println("+++ Query sent: " + query.encodePrettily());

        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        TypeReference<EnvironmentNodesResponse> typeRef =
                new TypeReference<EnvironmentNodesResponse>() {};
        EnvironmentNodesResponse actual = mapper.readValue(resp.bodyAsString(), typeRef);

        List<DiscoveryNode> expectedChildren = new ArrayList<>();
        DiscoveryNode expectedChild = new DiscoveryNode();
        expectedChild.name = "service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi";
        expectedChild.nodeType = "JVM";
        expectedChildren.add(expectedChild);

        DiscoveryNode expectedNode = new DiscoveryNode();
        expectedNode.name = "Custom Targets";
        expectedNode.nodeType = "Realm";
        expectedNode.children = expectedChildren;

        assertThat(actual.getData().getEnvironmentNodes().size(), is(1));

        DiscoveryNode actualNode = actual.getData().getEnvironmentNodes().get(0);

        assertThat(actualNode.name, is(expectedNode.name));
        assertThat(actualNode.nodeType, is(expectedNode.nodeType));
        assertThat(actualNode.children.size(), is(expectedNode.children.size()));

        for (int i = 0; i < actualNode.children.size(); i++) {
            DiscoveryNode actualChild = actualNode.children.get(i);
            DiscoveryNode expectedChildNode = expectedNode.children.get(i);
            assertThat(actualChild.name, is(expectedChildNode.name));
            assertThat(actualChild.nodeType, is(expectedChildNode.nodeType));
        }
    }

    @Disabled
    @Test
    @Order(1)
    void testOtherContainersFound() throws Exception {
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes { name nodeType labels target { alias connectUrl annotations {"
                        + " cryostat platform } } } }");
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        TargetNodesQueryResponse actual =
                mapper.readValue(resp.bodyAsString(), TargetNodesQueryResponse.class);
        MatcherAssert.assertThat(actual.data.targetNodes, Matchers.hasSize(1));

        TargetNode cryostat = new TargetNode();
        Target cryostatTarget = new Target();
        cryostatTarget.alias = "io.cryostat.Cryostat";
        cryostatTarget.connectUrl = "service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi";
        cryostat.name = cryostatTarget.connectUrl;
        cryostat.target = cryostatTarget;
        cryostat.nodeType = "JVM";
        Annotations cryostatAnnotations = new Annotations();
        cryostatAnnotations.cryostat =
                Map.of(
                        "REALM",
                        "JDP",
                        "JAVA_MAIN",
                        "io.cryostat.Cryostat",
                        "HOST",
                        "localhost",
                        "PORT",
                        "0");
        cryostatAnnotations.platform = Map.of();
        cryostatTarget.annotations = cryostatAnnotations;
        cryostat.labels = Map.of();
        MatcherAssert.assertThat(actual.data.targetNodes, Matchers.hasItem(cryostat));

        String uri = "service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi";
        String mainClass = "es.andrewazor.demo.Main";
        TargetNode ext = new TargetNode();
        Target target = new Target();
        target.alias = mainClass;
        target.connectUrl = uri;
        ext.name = target.connectUrl;
        ext.target = target;
        ext.nodeType = "JVM";
        Annotations annotations = new Annotations();
        annotations.cryostat =
                Map.of("REALM", "JDP", "JAVA_MAIN", mainClass, "HOST", "localhost", "PORT", "0");
        annotations.platform = Map.of();
        target.annotations = annotations;
        ext.labels = Map.of();
        MatcherAssert.assertThat(actual.data.targetNodes, Matchers.hasItem(ext));
    }

    @Disabled
    @Test
    @Order(2)
    void testQueryForSpecificTargetWithSpecificFields() throws Exception {
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes(filter: { annotations: \"PORT == 0\" }) { name nodeType }"
                        + " }");
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        TargetNodesQueryResponse actual =
                mapper.readValue(resp.bodyAsString(), TargetNodesQueryResponse.class);
        MatcherAssert.assertThat(actual.data.targetNodes, Matchers.hasSize(1));

        String uri = "service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi";
        TargetNode ext = new TargetNode();
        ext.name = uri;
        ext.nodeType = "JVM";
        MatcherAssert.assertThat(actual.data.targetNodes, Matchers.hasItem(ext));
    }

    @Disabled
    @Test
    @Order(3)
    void testStartRecordingMutationOnSpecificTarget() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes(filter: { annotations: \"PORT == 0\" }) {"
                    + " doStartRecording(recording: { name: \"graphql-itest\", duration: 30,"
                    + " template: \"Profiling\", templateType: \"TARGET\", archiveOnStop: true,"
                    + " metadata: { labels: [ { key: \"newLabel\", value: \"someValue\"} ] }  }) {"
                    + " name state duration archiveOnStop }} }");
        Map<String, String> expectedLabels =
                Map.of(
                        "template.name",
                        "Profiling",
                        "template.type",
                        "TARGET",
                        "newLabel",
                        "someValue");
        Future<JsonObject> f =
                worker.submit(
                        () -> {
                            try {
                                return expectNotification(
                                                "ActiveRecordingCreated", 15, TimeUnit.SECONDS)
                                        .get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                latch.countDown();
                            }
                        });

        Thread.sleep(5000); // Sleep to setup notification listening before query resolves

        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        StartRecordingMutationResponse actual =
                mapper.readValue(resp.bodyAsString(), StartRecordingMutationResponse.class);

        latch.await(30, TimeUnit.SECONDS);

        // Ensure ActiveRecordingCreated notification emitted matches expected values
        JsonObject notification = f.get(5, TimeUnit.SECONDS);

        JsonObject notificationRecording =
                notification.getJsonObject("message").getJsonObject("recording");
        MatcherAssert.assertThat(
                notificationRecording.getString("name"), Matchers.equalTo("graphql-itest"));
        MatcherAssert.assertThat(
                notificationRecording.getString("archiveOnStop"), Matchers.equalTo("true"));
        MatcherAssert.assertThat(
                notification.getJsonObject("message").getString("target"),
                Matchers.equalTo(
                        String.format(
                                "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", "localhost", 0)));
        Map<String, Object> notificationLabels =
                notificationRecording.getJsonObject("metadata").getJsonObject("labels").getMap();
        for (var entry : expectedLabels.entrySet()) {
            MatcherAssert.assertThat(
                    notificationLabels, Matchers.hasEntry(entry.getKey(), entry.getValue()));
        }

        RecordingNodes nodes = new RecordingNodes();

        ActiveRecording recording = new ActiveRecording();
        recording.name = "graphql-itest";
        recording.duration = 30_000L;
        recording.state = "RUNNING";
        recording.archiveOnStop = true;
        recording.metadata = RecordingMetadata.of(expectedLabels);

        StartRecording startRecording = new StartRecording();
        startRecording.doStartRecording = recording;

        nodes.targetNodes = List.of(startRecording);

        MatcherAssert.assertThat(actual.data, Matchers.equalTo(nodes));
    }

    @Disabled
    @Test
    @Order(4)
    void testArchiveMutation() throws Exception {
        Thread.sleep(5000);
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes(filter: { annotations: \"PORT == 0\" }) { recordings {"
                        + " active { data { name doArchive { name } } } } } }");
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        ArchiveMutationResponse actual =
                mapper.readValue(resp.bodyAsString(), ArchiveMutationResponse.class);

        MatcherAssert.assertThat(actual.data.targetNodes, Matchers.hasSize(1));

        TargetNode node = actual.data.targetNodes.get(0);

        MatcherAssert.assertThat(node.recordings.active.data, Matchers.hasSize(1));

        ActiveRecording activeRecording = node.recordings.active.data.get(0);

        MatcherAssert.assertThat(activeRecording.name, Matchers.equalTo("graphql-itest"));

        ArchivedRecording archivedRecording = activeRecording.doArchive;
        MatcherAssert.assertThat(
                archivedRecording.name,
                Matchers.matchesRegex(
                        "^es-andrewazor-demo-Main_graphql-itest_[0-9]{8}T[0-9]{6}Z\\.jfr$"));
    }

    @Disabled
    @Test
    @Order(5)
    void testActiveRecordingMetadataMutation() throws Exception {
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes(filter: { annotations: \"PORT == 9093\" }) {"
                        + "recordings { active {"
                        + " data {"
                        + " doPutMetadata(metadata: { labels: ["
                        + " {key:\"template.name\",value:\"Profiling\"},"
                        + " {key:\"template.type\",value:\"TARGET\"},"
                        + " {key:\"newLabel\",value:\"newValue\"}] })"
                        + " { metadata { labels } } } } } } }");
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        ActiveMutationResponse actual =
                mapper.readValue(resp.bodyAsString(), ActiveMutationResponse.class);

        MatcherAssert.assertThat(actual.data.targetNodes, Matchers.hasSize(1));

        TargetNode node = actual.data.targetNodes.get(0);

        MatcherAssert.assertThat(node.recordings.active.data, Matchers.hasSize(1));

        ActiveRecording activeRecording = node.recordings.active.data.get(0);

        MatcherAssert.assertThat(
                activeRecording.metadata,
                Matchers.equalTo(
                        RecordingMetadata.of(
                                Map.of(
                                        "template.name",
                                        "Profiling",
                                        "template.type",
                                        "TARGET",
                                        "newLabel",
                                        "newValue"))));
    }

    @Disabled
    @Test
    @Order(6)
    void testArchivedRecordingMetadataMutation() throws Exception {
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes(filter: { annotations: \"PORT == 0\" }) {"
                        + "recordings { archived {"
                        + " data { name size "
                        + " doPutMetadata(metadata: { labels: ["
                        + " {key:\"template.name\",value:\"Profiling\"},"
                        + " {key:\"template.type\",value:\"TARGET\"},"
                        + " {key:\"newArchivedLabel\",value:\"newArchivedValue\"}] })"
                        + " { metadata { labels } } } } } } }");
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        ArchiveMutationResponse actual =
                mapper.readValue(resp.bodyAsString(), ArchiveMutationResponse.class);

        MatcherAssert.assertThat(actual.data.targetNodes, Matchers.hasSize(1));

        TargetNode node = actual.data.targetNodes.get(0);

        MatcherAssert.assertThat(node.recordings.archived.data, Matchers.hasSize(1));

        ArchivedRecording archivedRecording = node.recordings.archived.data.get(0);
        MatcherAssert.assertThat(archivedRecording.size, Matchers.greaterThan(0L));

        MatcherAssert.assertThat(
                archivedRecording.metadata,
                Matchers.equalTo(
                        RecordingMetadata.of(
                                Map.of(
                                        "template.name",
                                        "Profiling",
                                        "template.type",
                                        "TARGET",
                                        "newArchivedLabel",
                                        "newArchivedValue"))));
    }

    @Disabled
    @Test
    @Order(7)
    void testDeleteMutation() throws Exception {
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes(filter: { annotations: \"PORT == 0\" }) { recordings {"
                        + " active { data { name doDelete { name }"
                        + " } aggregate { count } }"
                        + " archived { data { name doDelete { name }"
                        + " } aggregate { count size } }"
                        + " } } }");
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        DeleteMutationResponse actual =
                mapper.readValue(resp.bodyAsString(), DeleteMutationResponse.class);

        MatcherAssert.assertThat(actual.data.targetNodes, Matchers.hasSize(1));

        TargetNode node = actual.data.targetNodes.get(0);

        MatcherAssert.assertThat(node.recordings.active.data, Matchers.hasSize(1));
        MatcherAssert.assertThat(node.recordings.archived.data, Matchers.hasSize(1));
        MatcherAssert.assertThat(node.recordings.archived.aggregate.count, Matchers.equalTo(1L));
        MatcherAssert.assertThat(node.recordings.archived.aggregate.size, Matchers.greaterThan(0L));

        ActiveRecording activeRecording = node.recordings.active.data.get(0);
        ArchivedRecording archivedRecording = node.recordings.archived.data.get(0);

        MatcherAssert.assertThat(activeRecording.name, Matchers.equalTo("graphql-itest"));
        MatcherAssert.assertThat(activeRecording.doDelete.name, Matchers.equalTo("graphql-itest"));

        MatcherAssert.assertThat(
                archivedRecording.name,
                Matchers.matchesRegex(
                        "^es-andrewazor-demo-Main_graphql-itest_[0-9]{8}T[0-9]{6}Z\\.jfr$"));
    }

    /* @Disabled
    @Test
    @Order(8)
    void testNodesHaveIds() throws Exception {
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { environmentNodes(filter: { name: \"JDP\" }) { id descendantTargets { id }"
                        + " } }");
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        // if any of the nodes in the query did not have an ID property then the request
        // would fail
        EnvironmentNodesResponse actual =
                mapper.readValue(resp.bodyAsString(), EnvironmentNodesResponse.class);
        Set<Integer> observedIds = new HashSet<>();
        for (var env : actual.data.environmentNodes) {
            // ids should be unique
            MatcherAssert.assertThat(observedIds, Matchers.not(Matchers.contains(env.id)));
            observedIds.add(env.id);
            for (var target : env.descendantTargets) {
                MatcherAssert.assertThat(observedIds, Matchers.not(Matchers.contains(target.id)));
                observedIds.add(target.id);
            }
        }
    } */

    @Disabled
    @Test
    @Order(9)
    void testQueryForSpecificTargetsByNames() throws Exception {
        JsonObject query = new JsonObject();
        query.put(
                "query",
                String.format(
                        "query { targetNodes(filter: { names:"
                                + " [\"service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi\","
                                + " \"service:jmx:rmi:///jndi/rmi://localhost:9091/jmxrmi\"] }) {"
                                + " name nodeType } }"));
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        TargetNodesQueryResponse actual =
                mapper.readValue(resp.bodyAsString(), TargetNodesQueryResponse.class);
        List<TargetNode> targetNodes = actual.data.targetNodes;

        int expectedSize = 2;

        assertThat(targetNodes.size(), is(expectedSize));

        TargetNode target1 = new TargetNode();
        target1.name = "service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi";
        target1.nodeType = "JVM";
        TargetNode target2 = new TargetNode();
        target2.name = "service:jmx:rmi:///jndi/rmi://localhost:9091/jmxrmi";
        target2.nodeType = "JVM";

        assertThat(targetNodes, hasItem(target1));
        assertThat(targetNodes, hasItem(target2));
    }

    @Disabled
    @Test
    @Order(10)
    public void testQueryForFilteredActiveRecordingsByNames() throws Exception {
        // Check preconditions
        CompletableFuture<JsonArray> listRespFuture1 = new CompletableFuture<>();
        webClient
                .get(
                        String.format(
                                "/api/v1/targets/%s/recordings",
                                getSelfReferenceConnectUrlEncoded()))
                .send(
                        ar -> {
                            if (assertRequestStatus(ar, listRespFuture1)) {
                                listRespFuture1.complete(ar.result().bodyAsJsonArray());
                            }
                        });
        JsonArray listResp = listRespFuture1.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Assertions.assertTrue(listResp.isEmpty());

        // Create two new recordings
        CompletableFuture<Void> createRecordingFuture1 = new CompletableFuture<>();
        MultiMap form1 = MultiMap.caseInsensitiveMultiMap();
        form1.add("recordingName", "Recording1");
        form1.add("duration", "5");
        form1.add("events", "template=ALL");
        webClient
                .post(
                        String.format(
                                "/api/v1/targets/%s/recordings",
                                getSelfReferenceConnectUrlEncoded()))
                .sendForm(
                        form1,
                        ar -> {
                            if (assertRequestStatus(ar, createRecordingFuture1)) {
                                createRecordingFuture1.complete(null);
                            }
                        });
        createRecordingFuture1.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        CompletableFuture<Void> createRecordingFuture2 = new CompletableFuture<>();
        MultiMap form2 = MultiMap.caseInsensitiveMultiMap();
        form2.add("recordingName", "Recording2");
        form2.add("duration", "5");
        form2.add("events", "template=ALL");
        webClient
                .post(
                        String.format(
                                "/api/v1/targets/%s/recordings",
                                getSelfReferenceConnectUrlEncoded()))
                .sendForm(
                        form2,
                        ar -> {
                            if (assertRequestStatus(ar, createRecordingFuture2)) {
                                createRecordingFuture2.complete(null);
                            }
                        });
        createRecordingFuture2.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // GraphQL Query to filter Active recordings by names
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes (filter: {name:"
                    + " \"service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi\"}){ recordings"
                    + " {active(filter: { names: [\"Recording1\", \"Recording2\",\"Recording3\"] })"
                    + " {data {name}}}}}");
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        TargetNodesQueryResponse graphqlResp =
                mapper.readValue(resp.bodyAsString(), TargetNodesQueryResponse.class);

        List<String> filterNames = Arrays.asList("Recording1", "Recording2");

        List<ActiveRecording> filteredRecordings =
                graphqlResp.data.targetNodes.stream()
                        .flatMap(targetNode -> targetNode.recordings.active.data.stream())
                        .filter(recording -> filterNames.contains(recording.name))
                        .collect(Collectors.toList());

        MatcherAssert.assertThat(filteredRecordings.size(), Matchers.equalTo(2));
        ActiveRecording r1 = new ActiveRecording();
        r1.name = "Recording1";
        ActiveRecording r2 = new ActiveRecording();
        r2.name = "Recording2";

        assertThat(filteredRecordings, hasItem(r1));
        assertThat(filteredRecordings, hasItem(r2));

        // Delete recordings
        for (ActiveRecording recording : filteredRecordings) {
            String recordingName = recording.name;
            CompletableFuture<Void> deleteRecordingFuture = new CompletableFuture<>();
            webClient
                    .delete(
                            String.format(
                                    "/api/v1/targets/%s/recordings/%s",
                                    getSelfReferenceConnectUrlEncoded(), recordingName))
                    .send(
                            ar -> {
                                if (assertRequestStatus(ar, deleteRecordingFuture)) {
                                    deleteRecordingFuture.complete(null);
                                }
                            });
            deleteRecordingFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        // Verify no recordings available
        CompletableFuture<JsonArray> listRespFuture4 = new CompletableFuture<>();
        webClient
                .get(
                        String.format(
                                "/api/v1/targets/%s/recordings",
                                getSelfReferenceConnectUrlEncoded()))
                .send(
                        ar -> {
                            if (assertRequestStatus(ar, listRespFuture4)) {
                                listRespFuture4.complete(ar.result().bodyAsJsonArray());
                            }
                        });
        listResp = listRespFuture4.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        MatcherAssert.assertThat(
                "list should have size 0 after deleting recordings",
                listResp.size(),
                Matchers.equalTo(0));
    }

    @Disabled
    @Test
    @Order(11)
    public void shouldReturnArchivedRecordingsFilteredByNames() throws Exception {
        // Check preconditions
        CompletableFuture<JsonArray> listRespFuture1 = new CompletableFuture<>();
        webClient
                .get(
                        String.format(
                                "/api/v1/targets/%s/recordings",
                                getSelfReferenceConnectUrlEncoded()))
                .send(
                        ar -> {
                            if (assertRequestStatus(ar, listRespFuture1)) {
                                listRespFuture1.complete(ar.result().bodyAsJsonArray());
                            }
                        });
        JsonArray listResp = listRespFuture1.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Assertions.assertTrue(listResp.isEmpty());

        // Create a new recording
        CompletableFuture<Void> createRecordingFuture = new CompletableFuture<>();
        MultiMap form = MultiMap.caseInsensitiveMultiMap();
        form.add("recordingName", TEST_RECORDING_NAME);
        form.add("duration", "5");
        form.add("events", "template=ALL");
        webClient
                .post(
                        String.format(
                                "/api/v1/targets/%s/recordings",
                                getSelfReferenceConnectUrlEncoded()))
                .sendForm(
                        form,
                        ar -> {
                            if (assertRequestStatus(ar, createRecordingFuture)) {
                                createRecordingFuture.complete(null);
                            }
                        });
        createRecordingFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Archive the recording
        CompletableFuture<Void> archiveRecordingFuture = new CompletableFuture<>();
        webClient
                .patch(
                        String.format(
                                "/api/v1/targets/%s/recordings/%s",
                                getSelfReferenceConnectUrlEncoded(), TEST_RECORDING_NAME))
                .putHeader(HttpHeaders.CONTENT_TYPE.toString(), HttpMimeType.PLAINTEXT.mime())
                .sendBuffer(
                        Buffer.buffer("SAVE"),
                        ar -> {
                            if (assertRequestStatus(ar, archiveRecordingFuture)) {
                                archiveRecordingFuture.complete(null);
                            } else {

                                archiveRecordingFuture.completeExceptionally(
                                        new RuntimeException("Archive request failed"));
                            }
                        });

        archiveRecordingFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // retrieve to match the exact name
        CompletableFuture<JsonArray> archivedRecordingsFuture2 = new CompletableFuture<>();
        webClient
                .get(String.format("/api/v1/recordings"))
                .send(
                        ar -> {
                            if (assertRequestStatus(ar, archivedRecordingsFuture2)) {
                                archivedRecordingsFuture2.complete(ar.result().bodyAsJsonArray());
                            }
                        });
        JsonArray retrivedArchivedRecordings =
                archivedRecordingsFuture2.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        JsonObject retrievedArchivedrecordings = retrivedArchivedRecordings.getJsonObject(0);
        String retrievedArchivedRecordingsName = retrievedArchivedrecordings.getString("name");

        // GraphQL Query to filter Archived recordings by names
        CompletableFuture<TargetNodesQueryResponse> resp2 = new CompletableFuture<>();

        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes {"
                        + "recordings {"
                        + "archived(filter: { names: [\""
                        + retrievedArchivedRecordingsName
                        + "\",\"someOtherName\"] }) {"
                        + "data {"
                        + "name"
                        + "}"
                        + "}"
                        + "}"
                        + "}"
                        + "}");
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        TargetNodesQueryResponse graphqlResp =
                mapper.readValue(resp.bodyAsString(), TargetNodesQueryResponse.class);

        List<ArchivedRecording> archivedRecordings2 =
                graphqlResp.data.targetNodes.stream()
                        .flatMap(targetNode -> targetNode.recordings.archived.data.stream())
                        .collect(Collectors.toList());

        int filteredRecordingsCount = archivedRecordings2.size();
        Assertions.assertEquals(
                1, filteredRecordingsCount, "Number of filtered recordings should be 1");

        ArchivedRecording archivedRecording = archivedRecordings2.get(0);
        String filteredName = archivedRecording.name;
        Assertions.assertEquals(
                filteredName,
                retrievedArchivedRecordingsName,
                "Filtered name should match the archived recording name");

        // Delete archived recording by name
        for (ArchivedRecording archrecording : archivedRecordings2) {
            String nameMatch = archrecording.name;

            CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
            webClient
                    .delete(
                            String.format(
                                    "/api/beta/recordings/%s/%s",
                                    getSelfReferenceConnectUrlEncoded(), nameMatch))
                    .send(
                            ar -> {
                                if (assertRequestStatus(ar, deleteFuture)) {
                                    deleteFuture.complete(null);
                                } else {
                                    deleteFuture.completeExceptionally(
                                            new RuntimeException("Delete request failed"));
                                }
                            });

            deleteFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        // Retrieve the list of updated archived recordings to verify that the targeted
        // recordings
        // have been deleted
        CompletableFuture<JsonArray> updatedArchivedRecordingsFuture = new CompletableFuture<>();
        webClient
                .get("/api/v1/recordings")
                .send(
                        ar -> {
                            if (assertRequestStatus(ar, updatedArchivedRecordingsFuture)) {
                                updatedArchivedRecordingsFuture.complete(
                                        ar.result().bodyAsJsonArray());
                            }
                        });

        JsonArray updatedArchivedRecordings =
                updatedArchivedRecordingsFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Assert that the targeted recordings have been deleted
        boolean recordingsDeleted =
                updatedArchivedRecordings.stream()
                        .noneMatch(
                                json -> {
                                    JsonObject recording = (JsonObject) json;
                                    return recording.getString("name").equals(TEST_RECORDING_NAME);
                                });

        Assertions.assertTrue(
                recordingsDeleted, "The targeted archived recordings should be deleted");

        // Clean up what we created
        CompletableFuture<Void> deleteRespFuture1 = new CompletableFuture<>();
        webClient
                .delete(
                        String.format(
                                "/api/v1/targets/%s/recordings/%s",
                                getSelfReferenceConnectUrlEncoded(), TEST_RECORDING_NAME))
                .send(
                        ar -> {
                            if (assertRequestStatus(ar, deleteRespFuture1)) {
                                deleteRespFuture1.complete(null);
                            }
                        });

        deleteRespFuture1.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        CompletableFuture<JsonArray> savedRecordingsFuture = new CompletableFuture<>();
        webClient
                .get("/api/v1/recordings")
                .send(
                        ar -> {
                            if (assertRequestStatus(ar, savedRecordingsFuture)) {
                                savedRecordingsFuture.complete(ar.result().bodyAsJsonArray());
                            }
                        });

        JsonArray savedRecordings =
                savedRecordingsFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        for (Object savedRecording : savedRecordings) {
            String recordingName = ((JsonObject) savedRecording).getString("name");
            if (recordingName.matches("archivedRecordings")) {
                CompletableFuture<Void> deleteRespFuture2 = new CompletableFuture<>();
                webClient
                        .delete(
                                String.format(
                                        "/api/beta/recordings/%s/%s",
                                        getSelfReferenceConnectUrlEncoded(), recordingName))
                        .send(
                                ar -> {
                                    if (assertRequestStatus(ar, deleteRespFuture2)) {
                                        deleteRespFuture2.complete(null);
                                    }
                                });

                deleteRespFuture2.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }
    }

    /* @Disabled
    @Test
    @Order(12)
    public void testQueryforFilteredEnvironmentNodesByNames() throws Exception {
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { environmentNodes(filter: { names: [\"anotherName1\","
                        + " \"JDP\",\"anotherName2\"] }) { name nodeType } }");
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        EnvironmentNodesResponse actual =
                mapper.readValue(resp.bodyAsString(), EnvironmentNodesResponse.class);
        List<EnvironmentNode> environmentNodes = actual.data.environmentNodes;

        Assertions.assertEquals(1, environmentNodes.size(), "The list filtered should be 1");

        boolean nameExists = false;
        for (EnvironmentNode environmentNode : environmentNodes) {
            if (environmentNode.name.matches("JDP")) {
                nameExists = true;
                break;
            }
        }
        Assertions.assertTrue(nameExists, "Name not found");
    } */

    @Disabled
    @Test
    @Order(13)
    void testReplaceAlwaysOnStoppedRecording() throws Exception {
        try {
            // Start a Recording
            JsonObject notificationRecording = startRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));

            // Stop the Recording
            notificationRecording = stopRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("STOPPED", notificationRecording.getString("state"));

            // Restart the recording with replace:ALWAYS
            notificationRecording = restartRecording("ALWAYS");
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));
        } finally {
            // Delete the Recording
            deleteRecording();
        }
    }

    @Disabled
    @Test
    @Order(14)
    void testReplaceNeverOnStoppedRecording() throws Exception {
        try {
            // Start a Recording
            JsonObject notificationRecording = startRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));

            // Stop the Recording
            notificationRecording = stopRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("STOPPED", notificationRecording.getString("state"));

            // Restart the recording with replace:NEVER
            JsonObject error = restartRecordingWithError("NEVER");
            Assertions.assertTrue(
                    error.getString("message")
                            .contains("Recording with name \"test\" already exists"),
                    "Expected error message to contain 'Recording with name \"test\" already"
                            + " exists'");
        } finally {
            // Delete the Recording
            deleteRecording();
        }
    }

    @Disabled
    @Test
    @Order(15)
    void testReplaceStoppedOnStoppedRecording() throws Exception {
        try {
            // Start a Recording
            JsonObject notificationRecording = startRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));

            // Stop the Recording
            notificationRecording = stopRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("STOPPED", notificationRecording.getString("state"));

            // Restart the recording with replace:STOPPED
            notificationRecording = restartRecording("STOPPED");
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));
        } finally {
            // Delete the Recording
            deleteRecording();
        }
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = {"STOPPED", "NEVER"})
    @Order(16)
    void testReplaceStoppedOrNeverOnRunningRecording(String replace) throws Exception {
        try {
            // Start a Recording
            JsonObject notificationRecording = startRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));

            // Restart the recording with replace:NEVER
            JsonObject error = restartRecordingWithError("STOPPED");
            Assertions.assertTrue(
                    error.getString("message")
                            .contains("Recording with name \"test\" already exists"),
                    "Expected error message to contain 'Recording with name \"test\" already"
                            + " exists'");
        } finally {
            // Delete the Recording
            deleteRecording();
        }
    }

    @Disabled
    @Test
    @Order(17)
    void testReplaceAlwaysOnRunningRecording() throws Exception {
        try {
            // Start a Recording
            JsonObject notificationRecording = startRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));

            // Restart the recording with replace:ALWAYS
            notificationRecording = restartRecording("ALWAYS");
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));
        } finally {
            // Delete the Recording
            deleteRecording();
        }
    }

    @Disabled
    @Test
    @Order(18)
    void testRestartTrueOnRunningRecording() throws Exception {
        try {
            // Start a Recording
            JsonObject notificationRecording = startRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));

            // Restart the recording with replace:ALWAYS
            notificationRecording = restartRecording(true);
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));
        } finally {
            // Delete the Recording
            deleteRecording();
        }
    }

    @Disabled
    @Test
    @Order(19)
    void testRestartFalseOnRunningRecording() throws Exception {
        try {
            // Start a Recording
            JsonObject notificationRecording = startRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));

            // Restart the recording with replace:NEVER
            JsonObject error = restartRecordingWithError(false);
            Assertions.assertTrue(
                    error.getString("message")
                            .contains("Recording with name \"test\" already exists"),
                    "Expected error message to contain 'Recording with name \"test\" already"
                            + " exists'");
        } finally {
            // Delete the Recording
            deleteRecording();
        }
    }

    @Disabled
    @Test
    @Order(20)
    void testRestartTrueOnStoppedRecording() throws Exception {
        try {
            // Start a Recording
            JsonObject notificationRecording = startRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));

            // Stop the Recording
            notificationRecording = stopRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("STOPPED", notificationRecording.getString("state"));

            // Restart the recording with replace:ALWAYS
            notificationRecording = restartRecording(true);
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));
        } finally {
            // Delete the Recording
            deleteRecording();
        }
    }

    @Disabled
    @Test
    @Order(21)
    void testRestartFalseOnStoppedRecording() throws Exception {
        try {
            // Start a Recording
            JsonObject notificationRecording = startRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));

            // Stop the Recording
            notificationRecording = stopRecording();
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("STOPPED", notificationRecording.getString("state"));

            // Restart the recording with replace:NEVER
            JsonObject error = restartRecordingWithError(false);
            Assertions.assertTrue(
                    error.getString("message")
                            .contains("Recording with name \"test\" already exists"),
                    "Expected error message to contain 'Recording with name \"test\" already"
                            + " exists'");
        } finally {
            // Delete the Recording
            deleteRecording();
        }
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = {"ALWAYS", "STOPPED", "NEVER"})
    @Order(22)
    void testStartRecordingwithReplaceNever(String replace) throws Exception {
        try {
            JsonObject notificationRecording = restartRecording(replace);
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));
        } finally {
            // Delete the Recording
            deleteRecording();
        }
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @Order(23)
    void testRestartRecordingWithReplaceTrue(boolean restart) throws Exception {
        try {
            JsonObject notificationRecording = restartRecording(restart);
            Assertions.assertEquals("test", notificationRecording.getString("name"));
            Assertions.assertEquals("RUNNING", notificationRecording.getString("state"));
        } finally {
            // Delete the recording
            deleteRecording();
        }
    }

    static class Target {
        String alias;
        String connectUrl;
        Annotations annotations;

        @Override
        public int hashCode() {
            return Objects.hash(alias, connectUrl, annotations);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Target other = (Target) obj;
            return Objects.equals(alias, other.alias)
                    && Objects.equals(connectUrl, other.connectUrl)
                    && Objects.equals(annotations, other.annotations);
        }
    }

    static class Annotations {
        Map<String, String> platform;
        Map<String, String> cryostat;

        @Override
        public int hashCode() {
            return Objects.hash(cryostat, platform);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Annotations other = (Annotations) obj;
            return Objects.equals(cryostat, other.cryostat)
                    && Objects.equals(platform, other.platform);
        }
    }

    static class ArchivedRecording {
        String name;
        String reportUrl;
        String downloadUrl;
        RecordingMetadata metadata;
        long size;
        long archivedTime;

        ArchivedRecording doDelete;

        @Override
        public String toString() {
            return "ArchivedRecording [doDelete="
                    + doDelete
                    + ", downloadUrl="
                    + downloadUrl
                    + ", metadata="
                    + metadata
                    + ", name="
                    + name
                    + ", reportUrl="
                    + reportUrl
                    + ", size="
                    + size
                    + ", archivedTime="
                    + archivedTime
                    + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(doDelete, downloadUrl, metadata, name, reportUrl, size);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ArchivedRecording other = (ArchivedRecording) obj;
            return Objects.equals(doDelete, other.doDelete)
                    && Objects.equals(downloadUrl, other.downloadUrl)
                    && Objects.equals(metadata, other.metadata)
                    && Objects.equals(name, other.name)
                    && Objects.equals(reportUrl, other.reportUrl)
                    && Objects.equals(size, other.size);
        }
    }

    static class AggregateInfo {
        long count;
        long size;

        @Override
        public String toString() {
            return "AggregateInfo [count=" + count + ", size=" + size + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, size);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            AggregateInfo other = (AggregateInfo) obj;
            if (count != other.count) return false;
            if (size != other.size) return false;
            return true;
        }
    }

    static class Archived {
        List<ArchivedRecording> data;
        AggregateInfo aggregate;

        @Override
        public String toString() {
            return "Archived [data=" + data + ", aggregate=" + aggregate + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(data, aggregate);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Archived other = (Archived) obj;
            return Objects.equals(data, other.data) && Objects.equals(aggregate, other.aggregate);
        }
    }

    static class Recordings {
        Active active;
        Archived archived;

        @Override
        public String toString() {
            return "Recordings [active=" + active + ", archived=" + archived + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(active, archived);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Recordings other = (Recordings) obj;
            return Objects.equals(active, other.active) && Objects.equals(archived, other.archived);
        }
    }

    static class TargetNode {
        String name;
        String nodeType;
        Map<String, String> labels;
        Target target;
        Recordings recordings;
        ActiveRecording doStartRecording;

        @Override
        public String toString() {
            return "TargetNode [doStartRecording="
                    + doStartRecording
                    + ", labels="
                    + labels
                    + ", name="
                    + name
                    + ", nodeType="
                    + nodeType
                    + ", recordings="
                    + recordings
                    + ", target="
                    + target
                    + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(doStartRecording, labels, name, nodeType, recordings, target);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TargetNode other = (TargetNode) obj;
            return Objects.equals(doStartRecording, other.doStartRecording)
                    && Objects.equals(labels, other.labels)
                    && Objects.equals(name, other.name)
                    && Objects.equals(nodeType, other.nodeType)
                    && Objects.equals(recordings, other.recordings)
                    && Objects.equals(target, other.target);
        }
    }

    static class TargetNodes {

        List<TargetNode> targetNodes;

        @Override
        public int hashCode() {
            return Objects.hash(targetNodes);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TargetNodes other = (TargetNodes) obj;
            return Objects.equals(targetNodes, other.targetNodes);
        }

        @Override
        public String toString() {
            return "TargetNodes [targetNodes=" + targetNodes + "]";
        }
    }

    static class TargetNodesQueryResponse {
        TargetNodes data;

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            TargetNodesQueryResponse other = (TargetNodesQueryResponse) obj;
            return Objects.equals(data, other.data);
        }
    }

    static class Active {
        List<ActiveRecording> data;
        AggregateInfo aggregate;

        @Override
        public String toString() {
            return "Active [data=" + data + ", aggregate=" + aggregate + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(data, aggregate);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Active other = (Active) obj;
            return Objects.equals(data, other.data) && Objects.equals(aggregate, other.aggregate);
        }
    }

    static class ActiveRecording {
        String name;
        String reportUrl;
        String downloadUrl;
        RecordingMetadata metadata;
        String state;
        long startTime;
        long duration;
        boolean continuous;
        boolean toDisk;
        long maxSize;
        long maxAge;
        boolean archiveOnStop;

        ArchivedRecording doArchive;
        ActiveRecording doDelete;

        @Override
        public int hashCode() {
            return Objects.hash(
                    continuous,
                    doArchive,
                    doDelete,
                    downloadUrl,
                    duration,
                    maxAge,
                    maxSize,
                    archiveOnStop,
                    metadata,
                    name,
                    reportUrl,
                    startTime,
                    state,
                    toDisk);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ActiveRecording other = (ActiveRecording) obj;
            return continuous == other.continuous
                    && Objects.equals(doArchive, other.doArchive)
                    && Objects.equals(doDelete, other.doDelete)
                    && Objects.equals(downloadUrl, other.downloadUrl)
                    && duration == other.duration
                    && maxAge == other.maxAge
                    && maxSize == other.maxSize
                    && archiveOnStop == other.archiveOnStop
                    && Objects.equals(metadata, other.metadata)
                    && Objects.equals(name, other.name)
                    && Objects.equals(reportUrl, other.reportUrl)
                    && startTime == other.startTime
                    && Objects.equals(state, other.state)
                    && toDisk == other.toDisk;
        }

        @Override
        public String toString() {
            return "ActiveRecording [continuous="
                    + continuous
                    + ", doArchive="
                    + doArchive
                    + ", doDelete="
                    + doDelete
                    + ", downloadUrl="
                    + downloadUrl
                    + ", duration="
                    + duration
                    + ", maxAge="
                    + maxAge
                    + ", maxSize="
                    + maxSize
                    + ", archiveOnStop="
                    + archiveOnStop
                    + ", metadata="
                    + metadata
                    + ", name="
                    + name
                    + ", reportUrl="
                    + reportUrl
                    + ", startTime="
                    + startTime
                    + ", state="
                    + state
                    + ", toDisk="
                    + toDisk
                    + "]";
        }
    }

    static class RecordingMetadata {
        Map<String, String> labels;

        public static RecordingMetadata of(Map<String, String> of) {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(labels);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            RecordingMetadata other = (RecordingMetadata) obj;
            return Objects.equals(labels, other.labels);
        }

        @Override
        public String toString() {
            return "RecordingMetadata [labels=" + labels + "]";
        }
    }

    static class StartRecording {
        ActiveRecording doStartRecording;
        ArchivedRecording doArchive;
        ActiveRecording doPutMetadata;

        @Override
        public int hashCode() {
            return Objects.hash(doArchive, doStartRecording, doPutMetadata);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            StartRecording other = (StartRecording) obj;
            return Objects.equals(doArchive, other.doArchive)
                    && Objects.equals(doStartRecording, other.doStartRecording)
                    && Objects.equals(doPutMetadata, other.doPutMetadata);
        }

        @Override
        public String toString() {
            return "StartRecording [doArchive="
                    + doArchive
                    + ", doStartRecording="
                    + doStartRecording
                    + ", doPutMetadata="
                    + doPutMetadata
                    + "]";
        }
    }

    static class RecordingNodes {
        List<StartRecording> targetNodes;

        @Override
        public int hashCode() {
            return Objects.hash(targetNodes);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            RecordingNodes other = (RecordingNodes) obj;
            return Objects.equals(targetNodes, other.targetNodes);
        }

        @Override
        public String toString() {
            return "RecordingNodes [targetNodes=" + targetNodes + "]";
        }
    }

    static class StartRecordingMutationResponse {
        RecordingNodes data;

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            StartRecordingMutationResponse other = (StartRecordingMutationResponse) obj;
            return Objects.equals(data, other.data);
        }

        @Override
        public String toString() {
            return "StartRecordingMutationResponse [data=" + data + "]";
        }
    }

    static class Node {
        int id;
        String name;
        String nodeType;

        @Override
        public String toString() {
            return "Node [id=" + id + ", name=" + name + ", nodeType=" + nodeType + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, nodeType);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Node other = (Node) obj;
            return id == other.id
                    && Objects.equals(name, other.name)
                    && Objects.equals(nodeType, other.nodeType);
        }
    }

    static class EnvironmentNode extends Node {
        List<Node> children;

        @Override
        public String toString() {
            return "EnvironmentNode [children ="
                    + children
                    + ", name="
                    + name
                    + ", nodeType="
                    + nodeType
                    + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(children);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            EnvironmentNode other = (EnvironmentNode) obj;
            return Objects.equals(children, other.children);
        }
    }

    public class EnvironmentNodes {
        private List<EnvironmentNode> environmentNodes;

        public List<EnvironmentNode> getEnvironmentNodes() {
            return environmentNodes;
        }

        public void setEnvironmentNodes(List<EnvironmentNode> environmentNodes) {
            this.environmentNodes = environmentNodes;
        }

        @Override
        public String toString() {
            return "EnvironmentNodes [environmentNodes=" + environmentNodes + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(environmentNodes);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            EnvironmentNodes other = (EnvironmentNodes) obj;
            return Objects.equals(environmentNodes, other.environmentNodes);
        }
    }

    public static class EnvironmentNodesResponse {
        private Data data;

        public Data getData() {
            return data;
        }

        public void setData(Data data) {
            this.data = data;
        }

        public static class Data {
            private List<DiscoveryNode> environmentNodes;

            public List<DiscoveryNode> getEnvironmentNodes() {
                return environmentNodes;
            }

            public void setEnvironmentNodes(List<DiscoveryNode> environmentNodes) {
                this.environmentNodes = environmentNodes;
            }
        }
    }

    static class ArchiveMutationResponse {
        TargetNodes data;

        @Override
        public String toString() {
            return "ArchiveMutationResponse [data=" + data + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ArchiveMutationResponse other = (ArchiveMutationResponse) obj;
            return Objects.equals(data, other.data);
        }
    }

    static class ActiveMutationResponse extends ArchiveMutationResponse {
        @Override
        public String toString() {
            return "ActiveMutationResponse [data=" + data + "]";
        }
    }

    static class DeleteMutationResponse {
        TargetNodes data;

        @Override
        public String toString() {
            return "DeleteMutationResponse [data=" + data + "]";
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            DeleteMutationResponse other = (DeleteMutationResponse) obj;
            return Objects.equals(data, other.data);
        }
    }

    // start recording
    private JsonObject startRecording() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes(filter: {"
                        + " name:\"service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi\" }) {"
                        + " doStartRecording(recording: { name: \"test\", template:\"Profiling\","
                        + " templateType: \"TARGET\"}) { name state}} }");
        Future<JsonObject> f =
                worker.submit(
                        () -> {
                            try {
                                return expectNotification(
                                                "ActiveRecordingCreated", 15, TimeUnit.SECONDS)
                                        .get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                latch.countDown();
                            }
                        });

        Thread.sleep(5000);

        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        JsonObject notification = f.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return notification.getJsonObject("message").getJsonObject("recording");
    }

    // Stop the Recording
    private JsonObject stopRecording() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes(filter: { name:"
                        + " \"service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi\" })  {"
                        + " recordings { active { data { doStop { name state } } } } } }");

        Future<JsonObject> f2 =
                worker.submit(
                        () -> {
                            try {
                                return expectNotification(
                                                "ActiveRecordingStopped", 15, TimeUnit.SECONDS)
                                        .get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                latch.countDown();
                            }
                        });

        Thread.sleep(5000);

        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        JsonObject notification = f2.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return notification.getJsonObject("message").getJsonObject("recording");
    }

    // Delete the Recording
    private void deleteRecording() throws Exception {
        JsonObject query = new JsonObject();
        query.put(
                "query",
                "query { targetNodes(filter: { name:"
                        + " \"service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi\" }) {"
                        + " recordings { active { data { doDelete { name state } } } } } }");

        Thread.sleep(5000);

        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));
    }

    // Restart the recording with given replacement policy
    private JsonObject restartRecording(String replace) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        JsonObject query = new JsonObject();
        query.put(
                "query",
                String.format(
                        "query { targetNodes(filter: { name:"
                            + " \"service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi\" }) {"
                            + " doStartRecording(recording: { name: \"test\","
                            + " template:\"Profiling\", templateType: \"TARGET\", replace: %s}) {"
                            + " name state }} }",
                        replace));
        Future<JsonObject> f =
                worker.submit(
                        () -> {
                            try {
                                return expectNotification(
                                                "ActiveRecordingCreated", 15, TimeUnit.SECONDS)
                                        .get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                latch.countDown();
                            }
                        });

        Thread.sleep(5000);

        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        JsonObject notification = f.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return notification.getJsonObject("message").getJsonObject("recording");
    }

    private JsonObject restartRecording(boolean restart) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        JsonObject query = new JsonObject();
        query.put(
                "query",
                String.format(
                        "query { targetNodes(filter: { name:"
                            + " \"service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi\" }) {"
                            + " doStartRecording(recording: { name: \"test\","
                            + " template:\"Profiling\", templateType: \"TARGET\", restart: %b}) {"
                            + " name state }} }",
                        restart));
        Future<JsonObject> f =
                worker.submit(
                        () -> {
                            try {
                                return expectNotification(
                                                "ActiveRecordingCreated", 15, TimeUnit.SECONDS)
                                        .get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                latch.countDown();
                            }
                        });

        Thread.sleep(5000);

        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(200)).and(Matchers.lessThan(300)));

        latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        JsonObject notification = f.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return notification.getJsonObject("message").getJsonObject("recording");
    }

    private JsonObject restartRecordingWithError(String replace) throws Exception {
        JsonObject query = new JsonObject();

        query.put(
                "query",
                String.format(
                        "query { targetNodes(filter: { name:"
                                + " \"service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi\""
                                + " }) { doStartRecording(recording: { name: \"test\","
                                + " template:\"Profiling\", templateType: \"TARGET\", replace: %s})"
                                + " { name state }} }",
                        replace));
        Thread.sleep(5000);
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(400)).and(Matchers.lessThan(600)));

        JsonObject response = resp.bodyAsJsonObject();
        JsonArray errors = response.getJsonArray("errors");
        return errors.getJsonObject(0);
    }

    private JsonObject restartRecordingWithError(boolean restart) throws Exception {
        JsonObject query = new JsonObject();

        query.put(
                "query",
                String.format(
                        "query { targetNodes(filter: { name:"
                                + " \"service:jmx:rmi:///jndi/rmi://localhost:0/jmxrmi\""
                                + " }) { doStartRecording(recording: { name: \"test\","
                                + " template:\"Profiling\", templateType: \"TARGET\", restart: %b})"
                                + " { name state }} }",
                        restart));
        Thread.sleep(5000);
        HttpResponse<Buffer> resp =
                webClient
                        .extensions()
                        .post("/api/v2.2/graphql", query.toBuffer(), REQUEST_TIMEOUT_SECONDS);
        MatcherAssert.assertThat(
                resp.statusCode(),
                Matchers.both(Matchers.greaterThanOrEqualTo(400)).and(Matchers.lessThan(600)));

        JsonObject response = resp.bodyAsJsonObject();
        JsonArray errors = response.getJsonArray("errors");
        return errors.getJsonObject(0);
    }
}
