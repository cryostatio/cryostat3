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
package io.cryostat.targets;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.openjdk.jmc.common.unit.IConstrainedMap;
import org.openjdk.jmc.common.unit.IDescribedMap;
import org.openjdk.jmc.common.unit.IOptionDescriptor;
import org.openjdk.jmc.common.unit.IQuantity;
import org.openjdk.jmc.common.unit.ITypedQuantity;
import org.openjdk.jmc.common.unit.QuantityConversionException;
import org.openjdk.jmc.common.unit.UnitLookup;
import org.openjdk.jmc.flightrecorder.configuration.events.EventOptionID;
import org.openjdk.jmc.flightrecorder.configuration.events.IEventTypeID;
import org.openjdk.jmc.flightrecorder.configuration.internal.DefaultValueMap;
import org.openjdk.jmc.flightrecorder.configuration.internal.KnownEventOptions;
import org.openjdk.jmc.flightrecorder.configuration.internal.KnownRecordingOptions;
import org.openjdk.jmc.flightrecorder.configuration.recording.RecordingOptionsBuilder;
import org.openjdk.jmc.rjmx.ConnectionException;
import org.openjdk.jmc.rjmx.ServiceNotAvailableException;
import org.openjdk.jmc.rjmx.services.jfr.FlightRecorderException;
import org.openjdk.jmc.rjmx.services.jfr.IEventTypeInfo;
import org.openjdk.jmc.rjmx.services.jfr.IRecordingDescriptor;

import io.cryostat.core.EventOptionsBuilder.EventOptionException;
import io.cryostat.core.EventOptionsBuilder.EventTypeException;
import io.cryostat.core.net.CryostatFlightRecorderService;
import io.cryostat.core.templates.TemplateService;
import io.cryostat.core.templates.TemplateType;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.buffer.Buffer;
import org.jboss.logging.Logger;

class AgentJFRService implements CryostatFlightRecorderService {

    private final AgentClient client;
    private final TemplateService templateService;
    private final Logger logger = Logger.getLogger(getClass());

    AgentJFRService(AgentClient client, TemplateService templateService) {
        this.client = client;
        this.templateService = templateService;
    }

    @Override
    public IDescribedMap<EventOptionID> getDefaultEventOptions() {
        return KnownEventOptions.OPTION_DEFAULTS_V2;
    }

    @Override
    public IDescribedMap<String> getDefaultRecordingOptions() {
        return KnownRecordingOptions.OPTION_DEFAULTS_V2;
    }

    @Override
    public String getVersion() {
        return "agent"; // TODO
    }

    @Override
    public void close(IRecordingDescriptor descriptor) throws FlightRecorderException {
        client.deleteRecording(descriptor.getId()).await().atMost(Duration.ofSeconds(10));
    }

    @Override
    public void enable() throws FlightRecorderException {
        throw new UnimplementedException();
    }

    @Override
    public Collection<? extends IEventTypeInfo> getAvailableEventTypes()
            throws FlightRecorderException {
        return client.eventTypes().await().atMost(Duration.ofSeconds(10));
    }

    @Override
    public Map<String, IOptionDescriptor<?>> getAvailableRecordingOptions()
            throws FlightRecorderException {
        return KnownRecordingOptions.DESCRIPTORS_BY_KEY_V2;
    }

    @Override
    public List<IRecordingDescriptor> getAvailableRecordings() throws FlightRecorderException {
        return client.activeRecordings().await().atMost(Duration.ofSeconds(10));
    }

    @Override
    public IConstrainedMap<EventOptionID> getCurrentEventTypeSettings()
            throws FlightRecorderException {
        return Optional.of(client.eventSettings().await().atMost(Duration.ofSeconds(10)))
                .orElse(new DefaultValueMap<>(Map.of()));
    }

    @Override
    public IConstrainedMap<EventOptionID> getEventSettings(IRecordingDescriptor descriptor)
            throws FlightRecorderException {
        return new DefaultValueMap<>(Map.of());
    }

    @Override
    public Map<? extends IEventTypeID, ? extends IEventTypeInfo> getEventTypeInfoMapByID()
            throws FlightRecorderException {
        return Map.of();
    }

    @Override
    public IConstrainedMap<String> getRecordingOptions(IRecordingDescriptor descriptor)
            throws FlightRecorderException {
        return new DefaultValueMap<>(Map.of());
    }

    @Override
    public List<String> getServerTemplates() throws FlightRecorderException {
        return client.eventTemplates().await().atMost(Duration.ofSeconds(10));
    }

    @Override
    public IRecordingDescriptor getSnapshotRecording() throws FlightRecorderException {
        return client.startSnapshot().await().atMost(Duration.ofSeconds(10));
    }

    @Override
    public IRecordingDescriptor getUpdatedRecordingDescription(IRecordingDescriptor descriptor)
            throws FlightRecorderException {
        throw new UnimplementedException();
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public InputStream openStream(IRecordingDescriptor descriptor, boolean removeOnClose)
            throws FlightRecorderException {
        Uni<Buffer> u = client.openStream(descriptor.getId());
        Buffer b = u.await().atMost(Duration.ofSeconds(10));
        return new BufferedInputStream(new ByteArrayInputStream(b.getBytes()));
    }

    @Override
    public InputStream openStream(
            IRecordingDescriptor descriptor, IQuantity lastPartDuration, boolean removeOnClose)
            throws FlightRecorderException {
        throw new UnimplementedException();
    }

    @Override
    public InputStream openStream(
            IRecordingDescriptor descriptor,
            IQuantity startTime,
            IQuantity endTime,
            boolean removeOnClose)
            throws FlightRecorderException {
        throw new UnimplementedException();
    }

    @Override
    public IRecordingDescriptor start(
            IConstrainedMap<String> recordingOptions, IConstrainedMap<EventOptionID> eventOptions)
            throws FlightRecorderException {
        throw new UnimplementedException();
    }

    @Override
    public void stop(IRecordingDescriptor descriptor) throws FlightRecorderException {
        client.stopRecording(descriptor.getId()).await().atMost(Duration.ofSeconds(10));
    }

    @Override
    public void updateEventOptions(
            IRecordingDescriptor descriptor, IConstrainedMap<EventOptionID> eventOptions)
            throws FlightRecorderException {
        throw new UnimplementedException();
    }

    @Override
    public void updateRecordingOptions(
            IRecordingDescriptor descriptor, IConstrainedMap<String> newSettings)
            throws FlightRecorderException {
        client.updateRecordingOptions(descriptor.getId(), newSettings)
                .await()
                .atMost(Duration.ofSeconds(10));
    }

    @Override
    public IRecordingDescriptor start(
            IConstrainedMap<String> recordingOptions,
            String templateName,
            TemplateType preferredTemplateType)
            throws io.cryostat.core.FlightRecorderException,
                    FlightRecorderException,
                    ConnectionException,
                    IOException,
                    FlightRecorderException,
                    ServiceNotAvailableException,
                    QuantityConversionException,
                    EventOptionException,
                    EventTypeException {
        StartRecordingRequest req;
        String recordingName = recordingOptions.get("name").toString();
        long duration =
                (Optional.ofNullable(
                                        (ITypedQuantity)
                                                recordingOptions.get(
                                                        RecordingOptionsBuilder.KEY_DURATION))
                                .orElse(UnitLookup.MILLISECOND.quantity(0)))
                        .longValueIn(UnitLookup.MILLISECOND);
        long maxSize =
                (Optional.ofNullable(
                                        (ITypedQuantity)
                                                recordingOptions.get(
                                                        RecordingOptionsBuilder.KEY_MAX_SIZE))
                                .orElse(UnitLookup.BYTE.quantity(0)))
                        .longValueIn(UnitLookup.BYTE);
        long maxAge =
                (Optional.ofNullable(
                                        (ITypedQuantity)
                                                recordingOptions.get(
                                                        RecordingOptionsBuilder.KEY_MAX_AGE))
                                .orElse(UnitLookup.MILLISECOND.quantity(0)))
                        .longValueIn(UnitLookup.MILLISECOND);
        if (preferredTemplateType.equals(TemplateType.CUSTOM)) {
            req =
                    new StartRecordingRequest(
                            recordingName,
                            null,
                            templateService
                                    .getXml(templateName, preferredTemplateType)
                                    .orElseThrow()
                                    .outerHtml(),
                            duration,
                            maxSize,
                            maxAge);
        } else {
            req =
                    new StartRecordingRequest(
                            recordingName, templateName, null, duration, maxSize, maxAge);
        }
        return client.startRecording(req).await().atMost(Duration.ofSeconds(10));
    }

    public static class UnimplementedException extends IllegalStateException {}

    static record StartRecordingRequest(
            String name,
            String localTemplateName,
            String template,
            long duration,
            long maxSize,
            long maxAge) {}
}
