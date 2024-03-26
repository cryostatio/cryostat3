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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.projectnessie.cel.tools.ScriptException;

@ApplicationScoped
public class CredentialsFinder {

    @Inject MatchExpressionEvaluator expressionEvaluator;
    @Inject Logger logger;

    public Optional<Credential> getCredentialsForTarget(Target target) {
        return Credential.<Credential>listAll().stream()
                .filter(
                        c -> {
                            try {
                                return expressionEvaluator.applies(c.matchExpression, target);
                            } catch (ScriptException e) {
                                logger.error(e);
                                return false;
                            }
                        })
                .findFirst();
    }

    public Optional<Credential> getCredentialsForConnectUrl(URI connectUrl) {
        return Target.find("connectUrl", connectUrl)
                .<Target>firstResultOptional()
                .map(this::getCredentialsForTarget)
                .orElse(Optional.empty());
    }
}
