package com.kafka.producer.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/** Loads Java properties once and maps string keys into validated immutable configuration. */
public final class ConfigLoader {
    public ApplicationConfig load(Path path) throws IOException {
        if (path == null || !Files.isRegularFile(path)) {
            throw new IllegalArgumentException("Configuration file does not exist: " + path);
        }
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(path)) { p.load(in); }

        Path watch = Paths.get(required(p, "image.watch.directory")).toAbsolutePath().normalize();
        if (!Files.isDirectory(watch) || !Files.isReadable(watch)) {
            throw new IllegalArgumentException("Image watch directory must exist and be readable: " + watch);
        }
        Set<String> extensions = extensions(required(p, "image.allowed.extensions"));
        ApplicationConfig.ImageWatcher iw = new ApplicationConfig.ImageWatcher(watch, extensions,
                positiveLong(p, "image.file.stability.check.interval.ms", 500),
                positiveInt(p, "image.file.stability.required.count", 2),
                positiveLong(p, "image.file.stability.timeout.ms", 10000),
                positiveInt(p, "image.processing.thread.count", 2),
                positiveLong(p, "image.max.file.size.bytes", 52428800));

        ApplicationConfig.SecurityMode mode;
        try { mode = ApplicationConfig.SecurityMode.valueOf(required(p, "kafka.security.mode").toUpperCase(Locale.ROOT)); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("kafka.security.mode must be PLAINTEXT, SSL, or SASL_SSL", e); }
        String truststore = value(p, "kafka.ssl.truststore.location", "");
        if (!truststore.isEmpty()) requireFile("kafka.ssl.truststore.location", truststore);
        String mechanism = value(p, "kafka.sasl.mechanism", "SCRAM-SHA-512");
        String jaas = value(p, "kafka.sasl.jaas.config", "");
        if (mode == ApplicationConfig.SecurityMode.SASL_SSL) {
            if (!("SCRAM-SHA-256".equals(mechanism) || "SCRAM-SHA-512".equals(mechanism))) {
                throw new IllegalArgumentException("Only SCRAM-SHA-256 and SCRAM-SHA-512 are supported");
            }
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
        String systemId = required(p, "health.system.id");
        String programName = required(p, "health.program.name");
        ApplicationConfig.Producer imageProducer = producer(p, "image", deviceId + "-image", 0, 33554432L);
        ApplicationConfig.Producer healthProducer = producer(p, "health", deviceId + "-health",
                intValue(p, "health.kafka.linger.ms", 0),
                longValue(p, "health.kafka.buffer.memory", 1048576));
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
                deviceId, systemId, required(p, "health.source.type"),
                programName, required(p, "health.program.version"),
                required(p, "health.event.type"),
                "/systems/" + systemId + "/devices/" + deviceId + "/programs/" + programName,
                required(p, "health.event.dataschema"));
        return new ApplicationConfig(value(p, "application.name", "image-producer"),
                value(p, "application.version", "1.1-SNAPSHOT"),
                positiveInt(p, "application.fatal.exit.code", 20), iw, kc, imageProducer,
                healthProducer, health, identity);
    }

    private static ApplicationConfig.Producer producer(Properties p, String prefix, String clientId,
                                                       int linger, long buffer) {
        String kafkaPrefix = prefix + ".kafka.";
        String topicKey = "health".equals(prefix) ? "health.topic" : "image.kafka.topic";
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
        if (result.isEmpty()) throw new IllegalArgumentException("image.allowed.extensions must not be empty");
        return result;
    }

    private static void requireFile(String key, String value) {
        if (value == null || value.trim().isEmpty()) throw new IllegalArgumentException("Missing required property: " + key);
        Path path = Paths.get(value).toAbsolutePath().normalize();
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) throw new IllegalArgumentException(key + " must be a readable file: " + path);
    }
    private static String required(Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null || value.trim().isEmpty()) throw new IllegalArgumentException("Missing required property: " + key);
        return value.trim();
    }
    private static String value(Properties p, String key, String defaultValue) { return p.getProperty(key, defaultValue).trim(); }
    private static boolean bool(Properties p, String key, boolean d) { return Boolean.parseBoolean(value(p, key, String.valueOf(d))); }
    private static int intValue(Properties p, String key, int d) { try { return Integer.parseInt(value(p, key, String.valueOf(d))); } catch (NumberFormatException e) { throw new IllegalArgumentException(key + " must be an integer", e); } }
    private static long longValue(Properties p, String key, long d) { try { return Long.parseLong(value(p, key, String.valueOf(d))); } catch (NumberFormatException e) { throw new IllegalArgumentException(key + " must be an integer", e); } }
    private static int positiveInt(Properties p, String key, int d) { int v = intValue(p, key, d); if (v <= 0) throw new IllegalArgumentException(key + " must be > 0"); return v; }
    private static int nonNegativeInt(Properties p, String key, int d) { int v = intValue(p, key, d); if (v < 0) throw new IllegalArgumentException(key + " must be >= 0"); return v; }
    private static long positiveLong(Properties p, String key, long d) { long v = longValue(p, key, d); if (v <= 0) throw new IllegalArgumentException(key + " must be > 0"); return v; }
    private static long nonNegativeLong(Properties p, String key, long d) { long v = longValue(p, key, d); if (v < 0) throw new IllegalArgumentException(key + " must be >= 0"); return v; }
}
