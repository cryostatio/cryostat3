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

import java.util.Arrays;

import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumValueDefinition;
import graphql.schema.GraphQLSchema;
import jakarta.enterprise.event.Observes;
import jdk.jfr.RecordingState;
import org.eclipse.microprofile.graphql.GraphQLApi;

@GraphQLApi
public class SchemaExtension {

    public GraphQLSchema.Builder registerRecordingStateEnum(
            @Observes GraphQLSchema.Builder builder) {
        return createEnumType(
                builder, RecordingState.class, "Running state of an active Flight Recording");
    }

    private static GraphQLSchema.Builder createEnumType(
            GraphQLSchema.Builder builder, Class<? extends Enum<?>> klazz, String description) {
        return builder.additionalType(
                GraphQLEnumType.newEnum()
                        .name(klazz.getSimpleName())
                        .description(description)
                        .values(
                                Arrays.asList(klazz.getEnumConstants()).stream()
                                        .map(
                                                s ->
                                                        new GraphQLEnumValueDefinition.Builder()
                                                                .name(s.name())
                                                                .value(s)
                                                                .description(s.name())
                                                                .build())
                                        .toList())
                        .build());
    }
}
