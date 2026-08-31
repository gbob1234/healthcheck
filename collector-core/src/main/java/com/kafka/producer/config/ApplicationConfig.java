package com.kafka.producer.config;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

/** Immutable, validated configuration grouped by operational responsibility. */
public final class ApplicationConfig {
    public enum SecurityMode { PLAINTEXT, SSL, SASL_SSL }

    public static final class FileCollector {
        public final Path directory, archiveDirectory;
        public final boolean datedDirectoryMode;
        public final String dateDirectoryPattern, archiveDirectoryName;
        public final Set<String> extensions;
        public final long stabilityIntervalMs, stabilityTimeoutMs, maxFileSizeBytes, retryBackoffMs;
        public final int stabilityRequiredCount, threadCount, maxAttempts;

        public FileCollector(Path directory, Path archiveDirectory, Set<String> extensions,
                      long stabilityIntervalMs, int stabilityRequiredCount, long stabilityTimeoutMs,
                      int threadCount, long maxFileSizeBytes, int maxAttempts, long retryBackoffMs) {
            this(directory, archiveDirectory, false, null, null, extensions, stabilityIntervalMs,
                    stabilityRequiredCount, stabilityTimeoutMs, threadCount, maxFileSizeBytes,
                    maxAttempts, retryBackoffMs);
        }

        public FileCollector(Path rootDirectory, String dateDirectoryPattern, String archiveDirectoryName,
                      Set<String> extensions, long stabilityIntervalMs, int stabilityRequiredCount,
                      long stabilityTimeoutMs, int threadCount, long maxFileSizeBytes,
                      int maxAttempts, long retryBackoffMs) {
            this(rootDirectory, null, true, dateDirectoryPattern, archiveDirectoryName, extensions,
                    stabilityIntervalMs, stabilityRequiredCount, stabilityTimeoutMs, threadCount,
                    maxFileSizeBytes, maxAttempts, retryBackoffMs);
        }

        private FileCollector(Path directory, Path archiveDirectory, boolean datedDirectoryMode,
                      String dateDirectoryPattern, String archiveDirectoryName, Set<String> extensions,
                      long stabilityIntervalMs, int stabilityRequiredCount, long stabilityTimeoutMs,
                      int threadCount, long maxFileSizeBytes, int maxAttempts, long retryBackoffMs) {
            this.directory = directory;
            this.archiveDirectory = archiveDirectory;
            this.datedDirectoryMode = datedDirectoryMode;
            this.dateDirectoryPattern = dateDirectoryPattern;
            this.archiveDirectoryName = archiveDirectoryName;
            this.extensions = Collections.unmodifiableSet(extensions);
            this.stabilityIntervalMs = stabilityIntervalMs;
            this.stabilityRequiredCount = stabilityRequiredCount;
            this.stabilityTimeoutMs = stabilityTimeoutMs;
            this.threadCount = threadCount;
            this.maxFileSizeBytes = maxFileSizeBytes;
            this.maxAttempts = maxAttempts;
            this.retryBackoffMs = retryBackoffMs;
        }
    }

    public static final class S3 {
        public final URI endpoint;
        public final String region, bucket, objectKeyPrefix;
        public final boolean pathStyleAccessEnabled, tlsVerify;
        public final String accessKey, secretKey, sessionToken;
        public final long multipartThresholdBytes, multipartPartSizeBytes;

        S3(URI endpoint, String region, String bucket, String objectKeyPrefix,
           boolean pathStyleAccessEnabled, boolean tlsVerify,
           String accessKey, String secretKey, String sessionToken,
           long multipartThresholdBytes, long multipartPartSizeBytes) {
            this.endpoint = endpoint;
            this.region = region;
            this.bucket = bucket;
            this.objectKeyPrefix = objectKeyPrefix;
            this.pathStyleAccessEnabled = pathStyleAccessEnabled;
            this.tlsVerify = tlsVerify;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.sessionToken = sessionToken;
            this.multipartThresholdBytes = multipartThresholdBytes;
            this.multipartPartSizeBytes = multipartPartSizeBytes;
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
            this.bootstrapServers = bootstrapServers; this.securityMode = securityMode;
            this.truststoreLocation = truststoreLocation; this.truststorePassword = truststorePassword;
            this.keystoreLocation = keystoreLocation; this.keystorePassword = keystorePassword;
            this.keyPassword = keyPassword; this.endpointIdentificationAlgorithm = endpointIdentificationAlgorithm;
            this.saslMechanism = saslMechanism; this.saslUsername = saslUsername;
            this.saslPassword = saslPassword; this.saslJaasConfig = saslJaasConfig;
        }
    }

    public static final class Producer {
        public final String topic, clientId, acks;
        public final int retries, requestTimeoutMs, deliveryTimeoutMs, maxBlockMs, lingerMs;
        public final long bufferMemory;

        Producer(String topic, String clientId, String acks, int retries, int requestTimeoutMs,
                 int deliveryTimeoutMs, int maxBlockMs, int lingerMs, long bufferMemory) {
            this.topic = topic; this.clientId = clientId; this.acks = acks; this.retries = retries;
            this.requestTimeoutMs = requestTimeoutMs; this.deliveryTimeoutMs = deliveryTimeoutMs;
            this.maxBlockMs = maxBlockMs; this.lingerMs = lingerMs; this.bufferMemory = bufferMemory;
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
            this.enabled = enabled; this.required = required; this.intervalSeconds = intervalSeconds;
            this.initialDelaySeconds = initialDelaySeconds; this.workerStaleSeconds = workerStaleSeconds;
            this.initMaxAttempts = initMaxAttempts; this.initBackoffMs = initBackoffMs;
            this.maxConsecutiveFailures = maxConsecutiveFailures;
            this.maxFailureDurationSeconds = maxFailureDurationSeconds;
            this.recoveryEnabled = recoveryEnabled; this.recoveryMaxAttempts = recoveryMaxAttempts;
            this.recoveryBackoffMs = recoveryBackoffMs; this.shutdownSeconds = shutdownSeconds;
            this.fatalExitCode = fatalExitCode;
        }
    }

    public static final class Identity {
        public final String deviceId, deviceName, systemId, sourceType, programName, programVersion;
        public final String eventType, eventSource, eventDataSchema;

        public Identity(String deviceId, String deviceName, String systemId, String sourceType,
                 String programName, String programVersion, String eventType,
                 String eventSource, String eventDataSchema) {
            this.deviceId = deviceId; this.deviceName = deviceName; this.systemId = systemId;
            this.sourceType = sourceType; this.programName = programName;
            this.programVersion = programVersion; this.eventType = eventType;
            this.eventSource = eventSource; this.eventDataSchema = eventDataSchema;
        }
    }

    public final String applicationName, applicationVersion;
    public final int fatalExitCode;
    public final FileCollector fileCollector;
    public final S3 s3;
    public final KafkaCommon kafka;
    public final Producer fileProducer, healthProducer;
    public final Health health;
    public final Identity identity;

    ApplicationConfig(String applicationName, String applicationVersion, int fatalExitCode,
                      FileCollector fileCollector, S3 s3, KafkaCommon kafka, Producer fileProducer,
                      Producer healthProducer, Health health, Identity identity) {
        this.applicationName = applicationName; this.applicationVersion = applicationVersion;
        this.fatalExitCode = fatalExitCode; this.fileCollector = fileCollector; this.s3 = s3;
        this.kafka = kafka; this.fileProducer = fileProducer; this.healthProducer = healthProducer;
        this.health = health; this.identity = identity;
    }
}
