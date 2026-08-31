package com.kafka.producer.health;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.config.ApplicationConfig;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonCloudEventData;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Creates a spec-compliant CloudEvent while preserving the sample's data object structure. */
public final class HealthCloudEventFactory {
    private final ApplicationConfig config;
    private final ObjectMapper mapper;
    private final Clock clock;

    public HealthCloudEventFactory(ApplicationConfig config, ObjectMapper mapper, Clock clock) {
        this.config = config; this.mapper = mapper; this.clock = clock;
    }

    public CloudEvent create(HealthSnapshot s, HealthStatus status, long sequence) {
        Instant now = Instant.now(clock);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("sourceInfo", sourceInfo());
        data.put("status", status(status));
        data.put("heartbeat", heartbeat(s, sequence, now));
        data.put("kafkaInfo", kafkaInfo(s));
        data.put("workInfo", workInfo(s));
        data.put("errorInfo", errorInfo(s));
        JsonNode node = mapper.valueToTree(data);
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType(config.identity.eventType)
                .withSource(URI.create(config.identity.eventSource))
                .withSubject("healthcheck")
                .withTime(OffsetDateTime.ofInstant(now, ZoneOffset.UTC))
                .withDataContentType("application/json")
                .withDataSchema(URI.create(config.identity.eventDataSchema))
                .withData(JsonCloudEventData.wrap(node))
                .build();
    }

    private Map<String, Object> sourceInfo() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("systemId", config.identity.systemId); m.put("systemName", config.identity.systemId);
        m.put("sourceType", config.identity.sourceType); m.put("hostname", host()); m.put("ipAddress", address());
        m.put("osName", System.getProperty("os.name")); m.put("osVersion", System.getProperty("os.version"));
        m.put("javaVersion", System.getProperty("java.version")); m.put("programName", config.identity.programName);
        m.put("programVersion", config.identity.programVersion); m.put("processId", processId());
        m.put("instanceId", config.identity.deviceId); return m;
    }
    private Map<String, Object> status(HealthStatus value) { Map<String, Object> m = new LinkedHashMap<String, Object>(); m.put("level", value.level.name()); m.put("code", value.code); m.put("message", value.message); return m; }
    private Map<String, Object> heartbeat(HealthSnapshot s, long sequence, Instant now) { Map<String, Object> m = new LinkedHashMap<String, Object>(); m.put("sequence", sequence); m.put("interval", config.health.intervalSeconds); m.put("startedAt", text(s.programStartedAt)); m.put("uptimeSec", Duration.between(s.programStartedAt, now).getSeconds()); m.put("generatedAt", text(now)); return m; }
    private Map<String, Object> kafkaInfo(HealthSnapshot s) { Map<String, Object> m = new LinkedHashMap<String, Object>(); m.put("topicName", config.fileProducer.topic); m.put("lastSendSuccessAt", text(s.lastFileSendSuccessAt)); m.put("lastSendFailAt", text(s.lastFileSendFailureAt)); m.put("sendSuccessCount", s.fileSendSuccessCount); m.put("sendFailCount", s.fileSendFailureCount); return m; }
    private Map<String, Object> workInfo(HealthSnapshot s) { Map<String, Object> m = new LinkedHashMap<String, Object>(); m.put("lastFileDetectedAt", text(s.lastFileDetectedAt)); m.put("lastFileSentAt", text(s.lastFileSendSuccessAt)); return m; }
    private Map<String, Object> errorInfo(HealthSnapshot s) { Map<String, Object> m = new LinkedHashMap<String, Object>(); m.put("lastErrorAt", text(s.lastErrorAt)); m.put("code", s.lastErrorCode); m.put("lastErrorMessage", s.lastErrorMessage); return m; }
    private String text(Instant value) { return value == null ? null : value.toString(); }
    private String host() { try { return InetAddress.getLocalHost().getHostName(); } catch (Exception e) { return "unknown"; } }
    private String address() { try { return InetAddress.getLocalHost().getHostAddress(); } catch (Exception e) { return "unknown"; } }
    private String processId() { String name = ManagementFactory.getRuntimeMXBean().getName(); int at = name.indexOf('@'); return at > 0 ? name.substring(0, at) : name; }
}
