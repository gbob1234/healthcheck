package com.kafka.producer.image;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class ImageServicesTest {
    @TempDir Path temp;
    @Test void stabilizesEncodesAndBuildsJson() throws Exception {
        Path image = temp.resolve("sample.jpg"); Files.write(image, "image".getBytes(StandardCharsets.UTF_8));
        assertTrue(new ImageFileStabilityChecker(1, 1, 1000, 100).awaitStable(image));
        String encoded = new ImageEncodingService(100).encode(image);
        assertEquals(Base64.getEncoder().encodeToString("image".getBytes(StandardCharsets.UTF_8)), encoded);
        String json = new ImageMessageFactory(new ObjectMapper(), "DEVICE-1", Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)).create(image, encoded);
        JsonNode node = new ObjectMapper().readTree(json);
        assertEquals("sample.jpg", node.get("file_name").asText()); assertEquals("DEVICE-1", node.get("deviceId").asText()); assertEquals(encoded, node.get("image").asText());
    }
    @Test void rejectsEmptyAndOversizedFiles() throws Exception {
        Path empty = Files.createFile(temp.resolve("empty.png"));
        assertThrows(java.io.IOException.class, () -> new ImageEncodingService(10).encode(empty));
        Path large = temp.resolve("large.png"); Files.write(large, new byte[11]);
        assertThrows(java.io.IOException.class, () -> new ImageEncodingService(10).encode(large));
    }
}
