package com.kafka.producer.file;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.config.ApplicationConfig;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Builds a portable S3 metadata message without exposing the configured endpoint or credentials. */
public final class FileMetadataFactory {
    private final ObjectMapper mapper;
    private final ApplicationConfig.Identity identity;
    private final Clock clock;

    public FileMetadataFactory(ObjectMapper mapper, ApplicationConfig.Identity identity, Clock clock) {
        this.mapper = mapper; this.identity = identity; this.clock = clock;
    }
    public Metadata create(Path file, long size, String checksum, S3FileUploader.UploadResult uploaded)
            throws JsonProcessingException {
        String eventId = eventId(identity.deviceName, uploaded.bucket, uploaded.objectKey, checksum);
        Map<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("schemaVersion", 1);
        value.put("eventId", eventId);
        value.put("deviceName", identity.deviceName);
        value.put("fileName", file.getFileName().toString());
        value.put("fileType", extension(file).toUpperCase(Locale.ROOT));
        value.put("fileSize", size);
        value.put("checksumAlgorithm", "SHA-256");
        value.put("checksum", checksum);
        value.put("bucket", uploaded.bucket);
        value.put("objectKey", uploaded.objectKey);
        if (uploaded.eTag != null) value.put("eTag", uploaded.eTag);
        if (uploaded.versionId != null) value.put("versionId", uploaded.versionId);
        value.put("uploadedAt", Instant.now(clock).toString());
        return new Metadata(eventId, mapper.writeValueAsString(value));
    }
    private static String extension(Path file) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        return dot < 0 ? "" : name.substring(dot + 1);
    }
    private static String eventId(String device, String bucket, String key, String checksum) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest((device + "\n" + bucket + "\n" + key + "\n" + checksum)
                    .getBytes(StandardCharsets.UTF_8));
            StringBuilder value = new StringBuilder(64);
            for (byte b : bytes) value.append(String.format("%02x", b & 0xff));
            return value.toString();
        } catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 is unavailable", e); }
    }
    public static final class Metadata {
        public final String eventId, json;
        Metadata(String eventId, String json) { this.eventId = eventId; this.json = json; }
    }
}
