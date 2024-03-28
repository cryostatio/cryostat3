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
package io.cryostat.credentials;

import java.net.URI;
import java.util.Optional;

import io.cryostat.expressions.MatchExpressionEvaluator;
import io.cryostat.targets.Target;
import io.cryostat.targets.Target.EventKind;
import io.cryostat.targets.Target.TargetDiscovery;

import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.jboss.logging.Logger;
import org.projectnessie.cel.tools.ScriptException;

@ApplicationScoped
public class CredentialsFinder {

    @Inject MatchExpressionEvaluator expressionEvaluator;
    @Inject Logger logger;

    private final BidiMap<Target, Credential> cache = new DualHashBidiMap<>();

    @Blocking
    @ConsumeEvent(Credential.CREDENTIALS_DELETED)
    void onCredentialsDeleted(Credential credential) {
        cache.removeValue(credential);
    }

    @ConsumeEvent(Target.TARGET_JVM_DISCOVERY)
    void onMessage(TargetDiscovery event) {
        if (EventKind.LOST.equals(event.kind())) {
            cache.remove(event.serviceRef());
        }
    }

    @Blocking
    public Optional<Credential> getCredentialsForTarget(Target target) {
        return Optional.ofNullable(
                cache.computeIfAbsent(
                        target,
                        t ->
                                Credential.<Credential>listAll().stream()
                                        .filter(
                                                c -> {
                                                    try {
                                                        return expressionEvaluator.applies(
                                                                c.matchExpression, t);
                                                    } catch (ScriptException e) {
                                                        logger.error(e);
                                                        return false;
                                                    }
                                                })
                                        .findFirst()
                                        .orElse(null)));
    }

    @Blocking
    public Optional<Credential> getCredentialsForConnectUrl(URI connectUrl) {
        return Target.find("connectUrl", connectUrl)
                .<Target>singleResultOptional()
                .flatMap(this::getCredentialsForTarget);
    }
}
