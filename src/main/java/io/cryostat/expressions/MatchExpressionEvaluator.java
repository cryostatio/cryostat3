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
package io.cryostat.expressions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.cryostat.expressions.MatchExpression.ExpressionEvent;
import io.cryostat.targets.Target;
import io.cryostat.targets.Target.Annotations;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.quarkus.cache.CacheManager;
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CompositeCacheKey;
import io.quarkus.vertx.ConsumeEvent;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import org.jboss.logging.Logger;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;

@ApplicationScoped
public class MatchExpressionEvaluator {

    private static final String CACHE_NAME = "matchexpressions";

    @Inject ScriptHost scriptHost;
    @Inject Logger logger;
    @Inject CacheManager cacheManager;

    @ConsumeEvent(value = MatchExpression.EXPRESSION_ADDRESS, blocking = true)
    void onMessage(ExpressionEvent event) {
        switch (event.category()) {
            case CREATED:
                break;
            case DELETED:
                invalidate(event.expression().script);
                break;
            case UPDATED:
                // expression scripts aren't meant to be updatable, but handle them by invalidating
                // cached results just in case
                invalidate(event.expression().script);
                break;
            default:
                break;
        }
    }

    Script createScript(String matchExpression) throws ScriptCreateException {
        ScriptCreationEvent evt = new ScriptCreationEvent();
        try {
            evt.begin();
            return scriptHost
                    .buildScript(matchExpression)
                    .withDeclarations(
                            Decls.newVar(
                                    "target",
                                    Decls.newObjectType(SimplifiedTarget.class.getName())))
                    .withTypes(SimplifiedTarget.class)
                    .build();
        } finally {
            evt.end();
            if (evt.shouldCommit()) {
                evt.commit();
            }
        }
    }

    @CacheResult(cacheName = CACHE_NAME)
    boolean load(String matchExpression, Target target) throws ScriptException {
        Script script = createScript(matchExpression);
        return script.execute(Boolean.class, Map.of("target", SimplifiedTarget.from(target)));
    }

    void invalidate(String matchExpression) {
        var cache = cacheManager.getCache(CACHE_NAME).orElseThrow();
        // 0-index is important here. the argument order of the load() method determines the
        // composite key order
        cache.invalidateIf(
                        k ->
                                Objects.equals(
                                        (String) ((CompositeCacheKey) k).getKeyElements()[0],
                                        matchExpression))
                .subscribe()
                .with((v) -> {}, logger::warn);
    }

    public boolean applies(MatchExpression matchExpression, Target target) throws ScriptException {
        MatchExpressionAppliesEvent evt = new MatchExpressionAppliesEvent(matchExpression);
        try {
            evt.begin();
            return load(matchExpression.script, target);
        } catch (CompletionException e) {
            if (e.getCause() instanceof ScriptException) {
                throw (ScriptException) e.getCause();
            }
            throw e;
        } finally {
            evt.end();
            if (evt.shouldCommit()) {
                evt.commit();
            }
        }
    }

    public List<Target> getMatchedTargets(MatchExpression matchExpression) {
        try (Stream<Target> targets = Target.streamAll()) {
            return targets.filter(
                            target -> {
                                try {
                                    return applies(matchExpression, target);
                                } catch (ScriptException e) {
                                    logger.error(
                                            "Error while processing expression: " + matchExpression,
                                            e);
                                    return false;
                                }
                            })
                    .collect(Collectors.toList());
        }
    }

    @Name("io.cryostat.rules.MatchExpressionEvaluator.MatchExpressionAppliesEvent")
    @Label("Match Expression Evaluation")
    @Category("Cryostat")
    @SuppressFBWarnings(value = {"EI_EXPOSE_REP", "URF_UNREAD_FIELD"})
    public static class MatchExpressionAppliesEvent extends Event {

        String matchExpression;

        MatchExpressionAppliesEvent(MatchExpression matchExpression) {
            this.matchExpression = matchExpression.script;
        }
    }

    @Name("io.cryostat.rules.MatchExpressionEvaluator.ScriptCreationEvent")
    @Label("Match Expression Script Creation")
    @Category("Cryostat")
    // @SuppressFBWarnings(
    //         value = "URF_UNREAD_FIELD",
    //         justification = "The event fields are recorded with JFR instead of accessed
    // directly")
    public static class ScriptCreationEvent extends Event {}

    /**
     * Restricted view of a {@link io.cryostat.targets.Target} with only particular
     * expression-relevant fields exposed, connection URI exposed as a String, etc.
     */
    private static record SimplifiedTarget(
            String connectUrl,
            String alias,
            @Nullable String jvmId,
            Map<String, String> labels,
            Target.Annotations annotations) {
        SimplifiedTarget {
            Objects.requireNonNull(connectUrl);
            Objects.requireNonNull(alias);
            if (labels == null) {
                labels = Collections.emptyMap();
            }
            if (annotations == null) {
                annotations = new Annotations();
            }
        }

        static SimplifiedTarget from(Target target) {
            return new SimplifiedTarget(
                    target.connectUrl.toString(),
                    target.alias,
                    target.jvmId,
                    target.labels,
                    target.annotations);
        }
    }
}
