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
package io.cryostat.reports;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.openjdk.jmc.flightrecorder.rules.IRule;

import io.cryostat.core.reports.InterruptibleReportGenerator.AnalysisResult;
import io.cryostat.recordings.ActiveRecording;

public interface ReportsService {

    Future<Map<String, AnalysisResult>> reportFor(
            ActiveRecording recording, Predicate<IRule> predicate);

    default Future<Map<String, AnalysisResult>> reportFor(ActiveRecording recording) {
        return reportFor(recording, r -> true);
    }

    Future<Map<String, AnalysisResult>> reportFor(
            String jvmId, String filename, Predicate<IRule> predicate);

    default Future<Map<String, AnalysisResult>> reportFor(String jvmId, String filename) {
        return reportFor(jvmId, filename, r -> true);
    }
}
