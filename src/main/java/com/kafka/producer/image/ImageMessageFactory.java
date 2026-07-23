package com.kafka.producer.image;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Preserves the original image field and adds traceable filename, device, and creation time fields. */
public final class ImageMessageFactory {
    private final ObjectMapper mapper;
    private final String deviceId;
    private final Clock clock;

    public ImageMessageFactory(ObjectMapper mapper, String deviceId, Clock clock) {
        this.mapper = mapper;
        this.deviceId = deviceId;
        this.clock = clock;
    }

    public String create(Path file, String base64) throws JsonProcessingException {
        Map<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("file_name", file.getFileName().toString());
        value.put("create_time", Instant.now(clock).toString());
        value.put("deviceId", deviceId);
        value.put("image", base64);
        return mapper.writeValueAsString(value);
    }
}
