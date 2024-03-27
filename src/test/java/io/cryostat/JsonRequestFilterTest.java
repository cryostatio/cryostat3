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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class JsonRequestFilterTest {
    private JsonRequestFilter filter;
    private ContainerRequestContext requestContext;

    @BeforeEach
    void setUp() {
        filter = new JsonRequestFilter();
        filter.objectMapper = new ObjectMapper();
        requestContext = mock(ContainerRequestContext.class);
    }

    static Stream<String> payloadsReject() {
        return Stream.of(
                "{\"id\": 1}",
                "{\n  \"id\": 1\n}",
                "{\n  \"foo\": \"bar\",\n  \"id\": 1\n}",
                "[\n  {\n    \"id\": 1\n  }\n]");
    }

    @ParameterizedTest
    @MethodSource("payloadsReject")
    void testRejectsPayloadWithId(String payload) throws Exception {
        simulateRequest(payload);
        verify(requestContext, times(1)).abortWith(any(Response.class));
        reset(requestContext);
    }

    static Stream<String> payloadsAllow() {
        return Stream.of(
                "{ \"message\": \"this text includes the string literal \\\"id\\\"\" }",
                "{}",
                "[]",
                "{ \"foo\": \"bar\" }");
    }

    @ParameterizedTest
    @MethodSource("payloadsAllow")
    void testAllowsPayloadWithoutId(String payload) throws Exception {
        simulateRequest(payload);
        verify(requestContext, never()).abortWith(any(Response.class));
        reset(requestContext);
    }

    private void simulateRequest(String jsonPayload) throws Exception {
        ByteArrayInputStream payloadStream =
                new ByteArrayInputStream(jsonPayload.getBytes(StandardCharsets.UTF_8));
        when(requestContext.getEntityStream()).thenReturn(payloadStream);
        when(requestContext.getMediaType()).thenReturn(MediaType.APPLICATION_JSON_TYPE);
        UriInfo uriInfo = Mockito.mock(UriInfo.class);
        when(uriInfo.getPath()).thenReturn("/some/path");
        when(requestContext.getUriInfo()).thenReturn(uriInfo);
        filter.filter(requestContext);
    }
}
