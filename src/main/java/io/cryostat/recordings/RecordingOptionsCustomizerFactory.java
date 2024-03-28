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
package io.cryostat.recordings;

import java.util.HashMap;

import io.cryostat.core.RecordingOptionsCustomizer;
import io.cryostat.targets.Target;
import io.cryostat.targets.Target.EventKind;
import io.cryostat.targets.Target.TargetDiscovery;

import io.quarkus.vertx.ConsumeEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@ApplicationScoped
public class RecordingOptionsCustomizerFactory {

    @Inject Logger logger;

    private final HashMap<Target, RecordingOptionsCustomizer> customizers = new HashMap<>();

    @ConsumeEvent(Target.TARGET_JVM_DISCOVERY)
    void onMessage(TargetDiscovery event) {
        if (EventKind.LOST.equals(event.kind())) {
            customizers.remove(event.serviceRef());
        }
    }

    public RecordingOptionsCustomizer create(Target target) {
        return customizers.computeIfAbsent(
                target, (t) -> new RecordingOptionsCustomizer(logger::debug));
    }
}
