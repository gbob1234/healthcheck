package com.kafka.producer.health;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.TestConfigFactory;
import com.kafka.producer.config.ApplicationConfig;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class HealthCloudEventFactoryTest {
    @TempDir Path temp;
    @Test void matchesSampleEnvelopeAndDataFields() throws Exception {
        ApplicationConfig config = TestConfigFactory.load(temp, TestConfigFactory.base(temp));
        Clock clock = Clock.fixed(Instant.parse("2024-06-01T12:00:00Z"), ZoneOffset.UTC);
        ApplicationHealthState state = new ApplicationHealthState(clock);
        CloudEvent event = new HealthCloudEventFactory(config, new ObjectMapper(), clock).create(state.snapshot(), new HealthStatus(HealthStatus.Level.UP, "NORMAL", "ok"), 1);
        assertEquals("1.0", event.getSpecVersion().toString()); assertEquals("healthcheck", event.getSubject());
        assertEquals(config.identity.eventType, event.getType()); assertEquals("application/json", event.getDataContentType());
        JsonNode data = new ObjectMapper().readTree(event.getData().toBytes());
        assertTrue(data.has("sourceInfo")); assertTrue(data.has("status")); assertTrue(data.has("hearbeat"));
        assertTrue(data.has("kafkaInfo")); assertTrue(data.has("workInfo")); assertTrue(data.has("errorInfo"));
    }
}
