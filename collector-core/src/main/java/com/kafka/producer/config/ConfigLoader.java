package com.kafka.producer.config;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/** Loads Java properties once and maps string keys into validated immutable configuration. */
public final class ConfigLoader {
    private static final long MIN_MULTIPART_PART_SIZE = 5L * 1024L * 1024L;
    private static final long MAX_MULTIPART_PART_SIZE = 5L * 1024L * 1024L * 1024L;

    public ApplicationConfig load(Path path) throws IOException {
        if (path == null || !Files.isRegularFile(path))
            throw new IllegalArgumentException("Configuration file does not exist: " + path);
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(path)) { p.load(in); }

        Path watch = Paths.get(required(p, "file.watch.directory")).toAbsolutePath().normalize();
        if (!Files.isDirectory(watch) || !Files.isReadable(watch))
            throw new IllegalArgumentException("File watch directory must exist and be readable: " + watch);
        Path archive = Paths.get(required(p, "file.archive.directory")).toAbsolutePath().normalize();
        if (archive.equals(watch)) throw new IllegalArgumentException("file.archive.directory must differ from file.watch.directory");
        Files.createDirectories(archive);
        if (!Files.isDirectory(archive) || !Files.isWritable(archive))
            throw new IllegalArgumentException("File archive directory must be writable: " + archive);

        long maxFileSize = positiveLong(p, "file.max.file.size.bytes", 1099511627776L);
        ApplicationConfig.FileCollector fc = new ApplicationConfig.FileCollector(
                watch, archive, extensions(required(p, "file.allowed.extensions")),
                positiveLong(p, "file.stability.check.interval.ms", 500),
                positiveInt(p, "file.stability.required.count", 2),
                positiveLong(p, "file.stability.timeout.ms", 30000),
                positiveInt(p, "file.processing.thread.count", 1),
                maxFileSize,
                positiveInt(p, "file.processing.max.attempts", 3),
                nonNegativeLong(p, "file.processing.retry.backoff.ms", 5000));

        URI endpoint = uri(required(p, "s3.endpoint"), "s3.endpoint");
        String accessKey = value(p, "s3.access.key", "");
        String secretKey = value(p, "s3.secret.key", "");
        String sessionToken = value(p, "s3.session.token", "");
        if (accessKey.isEmpty() != secretKey.isEmpty())
            throw new IllegalArgumentException("s3.access.key and s3.secret.key must be configured together");
        if (!sessionToken.isEmpty() && accessKey.isEmpty())
            throw new IllegalArgumentException("s3.session.token requires s3.access.key and s3.secret.key");
        long partSize = positiveLong(p, "s3.multipart.part.size.bytes", 134217728L);
        if (partSize < MIN_MULTIPART_PART_SIZE || partSize > MAX_MULTIPART_PART_SIZE)
            throw new IllegalArgumentException("s3.multipart.part.size.bytes must be between 5242880 and 5368709120");
        if (maxFileSize > partSize * 10000L)
            throw new IllegalArgumentException("file.max.file.size.bytes exceeds the configured 10,000 multipart part capacity");
        ApplicationConfig.S3 s3 = new ApplicationConfig.S3(
                endpoint, required(p, "s3.region"), required(p, "s3.bucket"),
                normalizePrefix(value(p, "s3.object.key.prefix", "")),
                bool(p, "s3.path.style.access.enabled", false), bool(p, "s3.tls.verify", true),
                accessKey, secretKey, sessionToken,
                positiveLong(p, "s3.multipart.threshold.bytes", 67108864L), partSize);

        ApplicationConfig.SecurityMode mode;
        try { mode = ApplicationConfig.SecurityMode.valueOf(required(p, "kafka.security.mode").toUpperCase(Locale.ROOT)); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("kafka.security.mode must be PLAINTEXT, SSL, or SASL_SSL", e); }
        String truststore = value(p, "kafka.ssl.truststore.location", "");
        if (!truststore.isEmpty()) requireFile("kafka.ssl.truststore.location", truststore);
        String mechanism = value(p, "kafka.sasl.mechanism", "SCRAM-SHA-512");
        String jaas = value(p, "kafka.sasl.jaas.config", "");
        if (mode == ApplicationConfig.SecurityMode.SASL_SSL) {
            if (!("SCRAM-SHA-256".equals(mechanism) || "SCRAM-SHA-512".equals(mechanism)))
                throw new IllegalArgumentException("Only SCRAM-SHA-256 and SCRAM-SHA-512 are supported");
            if (jaas.isEmpty()) { required(p, "kafka.sasl.username"); required(p, "kafka.sasl.password"); }
        }
        String keystore = value(p, "kafka.ssl.keystore.location", "");
        if (!keystore.isEmpty()) requireFile("kafka.ssl.keystore.location", keystore);
        ApplicationConfig.KafkaCommon kc = new ApplicationConfig.KafkaCommon(
                required(p, "kafka.bootstrap.servers"), mode, truststore,
                value(p, "kafka.ssl.truststore.password", ""), keystore,
                value(p, "kafka.ssl.keystore.password", ""), value(p, "kafka.ssl.key.password", ""),
                value(p, "kafka.ssl.endpoint.identification.algorithm", "https"), mechanism,
                value(p, "kafka.sasl.username", ""), value(p, "kafka.sasl.password", ""), jaas);

        String deviceId = required(p, "device.id");
        String deviceName = value(p, "device.name", deviceId);
        String systemId = required(p, "health.system.id");
        String programName = required(p, "health.program.name");
        ApplicationConfig.Producer fileProducer = producer(p, "file", deviceId + "-file", 0, 33554432L);
        ApplicationConfig.Producer healthProducer = producer(p, "health", deviceId + "-health",
                intValue(p, "health.kafka.linger.ms", 0), longValue(p, "health.kafka.buffer.memory", 1048576));
        ApplicationConfig.Health health = new ApplicationConfig.Health(
                bool(p, "health.enabled", true), bool(p, "health.required", true),
                positiveLong(p, "health.interval.seconds", 60), nonNegativeLong(p, "health.initial.delay.seconds", 10),
                positiveLong(p, "health.worker.stale.threshold.seconds", 180),
                positiveInt(p, "health.init.max.attempts", 3), nonNegativeLong(p, "health.init.retry.backoff.ms", 5000),
                positiveInt(p, "health.send.max.consecutive.failures", 5),
                positiveLong(p, "health.send.max.failure.duration.seconds", 300),
                bool(p, "health.recovery.enabled", true), positiveInt(p, "health.recovery.max.attempts", 2),
                nonNegativeLong(p, "health.recovery.retry.backoff.ms", 5000),
                positiveLong(p, "health.shutdown.timeout.seconds", 5),
                positiveInt(p, "health.fatal.exit.code", positiveInt(p, "application.fatal.exit.code", 20)));
        ApplicationConfig.Identity identity = new ApplicationConfig.Identity(
                deviceId, deviceName, systemId, required(p, "health.source.type"), programName,
                required(p, "health.program.version"), required(p, "health.event.type"),
                "/systems/" + systemId + "/devices/" + deviceId + "/programs/" + programName,
                required(p, "health.event.dataschema"));
        return new ApplicationConfig(value(p, "application.name", "file-collector"),
                value(p, "application.version", "2.0-SNAPSHOT"),
                positiveInt(p, "application.fatal.exit.code", 20), fc, s3, kc, fileProducer,
                healthProducer, health, identity);
    }

    private static ApplicationConfig.Producer producer(Properties p, String prefix, String clientId,
                                                        int linger, long buffer) {
        String kafkaPrefix = prefix + ".kafka.";
        String topicKey = "health".equals(prefix) ? "health.topic" : "file.kafka.topic";
        return new ApplicationConfig.Producer(required(p, topicKey), clientId,
                value(p, kafkaPrefix + "acks", "health".equals(prefix) ? "1" : "all"),
                nonNegativeInt(p, kafkaPrefix + "retries", "health".equals(prefix) ? 1 : 3),
                positiveInt(p, kafkaPrefix + "request.timeout.ms", "health".equals(prefix) ? 5000 : 30000),
                positiveInt(p, kafkaPrefix + "delivery.timeout.ms", "health".equals(prefix) ? 10000 : 120000),
                positiveInt(p, kafkaPrefix + "max.block.ms", "health".equals(prefix) ? 3000 : 60000), linger, buffer);
    }

    private static Set<String> extensions(String raw) {
        Set<String> result = new HashSet<String>();
        for (String item : raw.split(",")) {
            String value = item.trim().toLowerCase(Locale.ROOT);
            if (value.startsWith(".")) value = value.substring(1);
            if (!value.isEmpty()) result.add(value);
        }
        if (result.isEmpty()) throw new IllegalArgumentException("file.allowed.extensions must not be empty");
        return result;
    }

    private static String normalizePrefix(String raw) {
        String value = raw.trim().replace('\\', '/');
        while (value.startsWith("/")) value = value.substring(1);
        while (value.endsWith("/")) value = value.substring(0, value.length() - 1);
        if (value.contains("../") || value.equals("..")) throw new IllegalArgumentException("s3.object.key.prefix must not contain '..'");
        return value;
    }

    private static URI uri(String raw, String key) {
        try {
            URI uri = URI.create(raw);
            if (uri.getScheme() == null || uri.getHost() == null) throw new IllegalArgumentException();
            return uri;
        } catch (Exception e) { throw new IllegalArgumentException(key + " must be an absolute URI", e); }
    }

    private static void requireFile(String key, String value) {
        Path path = Paths.get(value).toAbsolutePath().normalize();
        if (!Files.isRegularFile(path) || !Files.isReadable(path))
            throw new IllegalArgumentException(key + " must be a readable file: " + path);
    }
    private static String required(Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null || value.trim().isEmpty()) throw new IllegalArgumentException("Missing required property: " + key);
        return value.trim();
    }
    private static String value(Properties p, String key, String d) { return p.getProperty(key, d).trim(); }
    private static boolean bool(Properties p, String key, boolean d) { return Boolean.parseBoolean(value(p, key, String.valueOf(d))); }
    private static int intValue(Properties p, String key, int d) { try { return Integer.parseInt(value(p, key, String.valueOf(d))); } catch (NumberFormatException e) { throw new IllegalArgumentException(key + " must be an integer", e); } }
    private static long longValue(Properties p, String key, long d) { try { return Long.parseLong(value(p, key, String.valueOf(d))); } catch (NumberFormatException e) { throw new IllegalArgumentException(key + " must be an integer", e); } }
    private static int positiveInt(Properties p, String key, int d) { int v = intValue(p, key, d); if (v <= 0) throw new IllegalArgumentException(key + " must be > 0"); return v; }
    private static int nonNegativeInt(Properties p, String key, int d) { int v = intValue(p, key, d); if (v < 0) throw new IllegalArgumentException(key + " must be >= 0"); return v; }
    private static long positiveLong(Properties p, String key, long d) { long v = longValue(p, key, d); if (v <= 0) throw new IllegalArgumentException(key + " must be > 0"); return v; }
    private static long nonNegativeLong(Properties p, String key, long d) { long v = longValue(p, key, d); if (v < 0) throw new IllegalArgumentException(key + " must be >= 0"); return v; }
}
