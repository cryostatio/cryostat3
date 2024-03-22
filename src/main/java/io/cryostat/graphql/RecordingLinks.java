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

import io.cryostat.recordings.ActiveRecording;
import io.cryostat.recordings.RecordingHelper;
import io.cryostat.recordings.Recordings.ArchivedRecording;

import jakarta.inject.Inject;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Source;

@GraphQLApi
public class RecordingLinks {

    @Inject RecordingHelper recordingHelper;

    @Description("URL for GET request to retrieve the JFR binary file content of this recording")
    public String downloadUrl(@Source ActiveRecording recording) {
        return recordingHelper.downloadUrl(recording);
    }

    @Description(
            "URL for GET request to retrieve a JSON formatted Automated Analysis Report of this"
                    + " recording")
    public String reportUrl(@Source ActiveRecording recording) {
        return recordingHelper.reportUrl(recording);
    }

    @Description("URL for GET request to retrieve the JFR binary file content of this recording")
    public String downloadUrl(@Source ArchivedRecording recording) {
        return recording.downloadUrl();
    }

    @Description(
            "URL for GET request to retrieve a JSON formatted Automated Analysis Report of this"
                    + " recording")
    public String reportUrl(@Source ArchivedRecording recording) {
        return recording.reportUrl();
    }
}
