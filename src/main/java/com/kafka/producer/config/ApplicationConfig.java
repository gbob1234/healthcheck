package com.kafka.producer.config;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

/** Immutable, validated configuration grouped by operational responsibility. */
public final class ApplicationConfig {
    public enum SecurityMode { PLAINTEXT, SSL, SASL_SSL }

    public static final class ImageWatcher {
        public final Path directory;
        public final Set<String> extensions;
        public final long stabilityIntervalMs;
        public final int stabilityRequiredCount;
        public final long stabilityTimeoutMs;
        public final int threadCount;
        public final long maxFileSizeBytes;

        ImageWatcher(Path directory, Set<String> extensions, long stabilityIntervalMs,
                     int stabilityRequiredCount, long stabilityTimeoutMs, int threadCount,
                     long maxFileSizeBytes) {
            this.directory = directory;
            this.extensions = Collections.unmodifiableSet(extensions);
            this.stabilityIntervalMs = stabilityIntervalMs;
            this.stabilityRequiredCount = stabilityRequiredCount;
            this.stabilityTimeoutMs = stabilityTimeoutMs;
            this.threadCount = threadCount;
            this.maxFileSizeBytes = maxFileSizeBytes;
        }
    }

    public static final class KafkaCommon {
        public final String bootstrapServers;
        public final SecurityMode securityMode;
        public final String truststoreLocation, truststorePassword;
        public final String keystoreLocation, keystorePassword, keyPassword;
        public final String endpointIdentificationAlgorithm;
        public final String saslMechanism, saslUsername, saslPassword, saslJaasConfig;

        KafkaCommon(String bootstrapServers, SecurityMode securityMode, String truststoreLocation,
                    String truststorePassword, String keystoreLocation, String keystorePassword,
                    String keyPassword, String endpointIdentificationAlgorithm,
                    String saslMechanism, String saslUsername, String saslPassword,
                    String saslJaasConfig) {
            this.bootstrapServers = bootstrapServers;
            this.securityMode = securityMode;
            this.truststoreLocation = truststoreLocation;
            this.truststorePassword = truststorePassword;
            this.keystoreLocation = keystoreLocation;
            this.keystorePassword = keystorePassword;
            this.keyPassword = keyPassword;
            this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
            this.saslMechanism = saslMechanism;
            this.saslUsername = saslUsername;
            this.saslPassword = saslPassword;
            this.saslJaasConfig = saslJaasConfig;
        }
    }

    public static final class Producer {
        public final String topic, clientId, acks;
        public final int retries, requestTimeoutMs, deliveryTimeoutMs, maxBlockMs;
        public final int lingerMs;
        public final long bufferMemory;

        Producer(String topic, String clientId, String acks, int retries, int requestTimeoutMs,
                 int deliveryTimeoutMs, int maxBlockMs, int lingerMs, long bufferMemory) {
            this.topic = topic;
            this.clientId = clientId;
            this.acks = acks;
            this.retries = retries;
            this.requestTimeoutMs = requestTimeoutMs;
            this.deliveryTimeoutMs = deliveryTimeoutMs;
            this.maxBlockMs = maxBlockMs;
            this.lingerMs = lingerMs;
            this.bufferMemory = bufferMemory;
        }
    }

    public static final class Health {
        public final boolean enabled, required, recoveryEnabled;
        public final long intervalSeconds, initialDelaySeconds, workerStaleSeconds;
        public final int initMaxAttempts, maxConsecutiveFailures, recoveryMaxAttempts;
        public final long initBackoffMs, maxFailureDurationSeconds, recoveryBackoffMs, shutdownSeconds;
        public final int fatalExitCode;

        Health(boolean enabled, boolean required, long intervalSeconds, long initialDelaySeconds,
               long workerStaleSeconds, int initMaxAttempts, long initBackoffMs,
               int maxConsecutiveFailures, long maxFailureDurationSeconds, boolean recoveryEnabled,
               int recoveryMaxAttempts, long recoveryBackoffMs, long shutdownSeconds, int fatalExitCode) {
            this.enabled = enabled;
            this.required = required;
            this.intervalSeconds = intervalSeconds;
            this.initialDelaySeconds = initialDelaySeconds;
            this.workerStaleSeconds = workerStaleSeconds;
            this.initMaxAttempts = initMaxAttempts;
            this.initBackoffMs = initBackoffMs;
            this.maxConsecutiveFailures = maxConsecutiveFailures;
            this.maxFailureDurationSeconds = maxFailureDurationSeconds;
            this.recoveryEnabled = recoveryEnabled;
            this.recoveryMaxAttempts = recoveryMaxAttempts;
            this.recoveryBackoffMs = recoveryBackoffMs;
            this.shutdownSeconds = shutdownSeconds;
            this.fatalExitCode = fatalExitCode;
        }
    }

    public static final class Identity {
        public final String deviceId, systemId, sourceType, programName, programVersion;
        public final String eventType, eventSource, eventDataSchema;

        Identity(String deviceId, String systemId, String sourceType, String programName,
                 String programVersion, String eventType, String eventSource, String eventDataSchema) {
            this.deviceId = deviceId;
            this.systemId = systemId;
            this.sourceType = sourceType;
            this.programName = programName;
            this.programVersion = programVersion;
            this.eventType = eventType;
            this.eventSource = eventSource;
            this.eventDataSchema = eventDataSchema;
        }
    }

    public final String applicationName, applicationVersion;
    public final int fatalExitCode;
    public final ImageWatcher imageWatcher;
    public final KafkaCommon kafka;
    public final Producer imageProducer, healthProducer;
    public final Health health;
    public final Identity identity;

    ApplicationConfig(String applicationName, String applicationVersion, int fatalExitCode,
                      ImageWatcher imageWatcher, KafkaCommon kafka, Producer imageProducer,
                      Producer healthProducer, Health health, Identity identity) {
        this.applicationName = applicationName;
        this.applicationVersion = applicationVersion;
        this.fatalExitCode = fatalExitCode;
        this.imageWatcher = imageWatcher;
        this.kafka = kafka;
        this.imageProducer = imageProducer;
        this.healthProducer = healthProducer;
        this.health = health;
        this.identity = identity;
    }
}
