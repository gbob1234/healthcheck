package com.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.config.ApplicationConfig;
import com.kafka.producer.config.ConfigLoader;
import com.kafka.producer.file.AwsS3FileUploader;
import com.kafka.producer.file.FileChecksum;
import com.kafka.producer.file.FileDirectoryWatcher;
import com.kafka.producer.file.FileMetadataFactory;
import com.kafka.producer.file.FileMetadataPublisher;
import com.kafka.producer.file.FileStabilityChecker;
import com.kafka.producer.file.ObjectKeyFactory;
import com.kafka.producer.health.ApplicationHealthState;
import com.kafka.producer.health.HealthCloudEventFactory;
import com.kafka.producer.health.HealthKafkaPublisher;
import com.kafka.producer.health.HealthReporter;
import com.kafka.producer.health.HealthStatusEvaluator;
import com.kafka.producer.kafka.KafkaPropertiesFactory;
import com.kafka.producer.lifecycle.ApplicationLifecycleManager;
import io.cloudevents.jackson.JsonFormat;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Composition root for S3 file collection, Kafka metadata, health reporting, and lifecycle. */
public final class ApplicationMain {
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMain.class);

  private ApplicationMain() {}

  public static void main(String[] args) {
    final ApplicationLifecycleManager earlyLifecycle = new ApplicationLifecycleManager(20);
    if (args.length != 1) {
      earlyLifecycle.terminate(
          "INVALID_ARGUMENTS",
          "Usage: java -jar file-collector-2.0-SNAPSHOT.jar <config.properties>",
          null);
      return;
    }
    try {
      start(Paths.get(args[0]).toAbsolutePath().normalize());
    } catch (Exception e) {
      earlyLifecycle.terminate("STARTUP_FAILED", e.getMessage(), e);
    }
  }

  private static void start(Path configPath) throws Exception {
    ApplicationConfig config = new ConfigLoader().load(configPath);
    final ApplicationLifecycleManager lifecycle =
        new ApplicationLifecycleManager(config.fatalExitCode);
    try {
      Clock clock = Clock.systemDefaultZone();
      ObjectMapper mapper = new ObjectMapper();
      mapper.registerModule(JsonFormat.getCloudEventJacksonModule());
      ApplicationHealthState state = new ApplicationHealthState(clock);
      KafkaPropertiesFactory kafkaFactory = new KafkaPropertiesFactory();
      Properties fileProperties = kafkaFactory.create(config.kafka, config.fileProducer);
      Properties healthProperties = kafkaFactory.create(config.kafka, config.healthProducer);

      LOG.info(
          "Loaded configuration: file={}, watchDirectory={}, targetTemplate={}, archive={},"
              + " datedMode={}, securityMode={}, fileTopic={}, healthTopic={}, s3Endpoint={},"
              + " s3Bucket={}, s3Prefix={}, s3PathStyle={}, s3TlsVerify={}",
          configPath,
          config.fileCollector.directory,
          config.fileCollector.targetDirectoryTemplate,
          config.fileCollector.datedDirectoryMode
              ? config.fileCollector.archiveDirectoryName
              : config.fileCollector.archiveDirectory,
          config.fileCollector.datedDirectoryMode,
          config.kafka.securityMode,
          config.fileProducer.topic,
          config.healthProducer.topic,
          config.s3.endpoint,
          config.s3.bucket,
          config.s3.objectKeyPrefix,
          config.s3.pathStyleAccessEnabled,
          config.s3.tlsVerify);
      AwsS3FileUploader uploader = new AwsS3FileUploader(config.s3);
      lifecycle.register(null, null, null, null, uploader);
      FileMetadataPublisher filePublisher =
          new FileMetadataPublisher(fileProperties, config.fileProducer.topic, state);
      lifecycle.register(null, null, null, filePublisher, uploader);
      HealthKafkaPublisher healthPublisher =
          initializeHealthPublisher(config, healthProperties, mapper, lifecycle);
      if (config.health.enabled && healthPublisher == null && config.health.required) return;
      FileDirectoryWatcher watcher =
          new FileDirectoryWatcher(
              config.fileCollector,
              new FileStabilityChecker(
                  config.fileCollector.stabilityIntervalMs,
                  config.fileCollector.stabilityRequiredCount,
                  config.fileCollector.stabilityTimeoutMs,
                  config.fileCollector.maxFileSizeBytes),
              new FileChecksum(),
              new ObjectKeyFactory(config.s3.objectKeyPrefix),
              uploader,
              new FileMetadataFactory(mapper, config.identity, clock),
              filePublisher,
              state,
              lifecycle,
              clock);
      HealthReporter reporter =
          healthPublisher == null
              ? null
              : new HealthReporter(
                  config,
                  state,
                  new HealthStatusEvaluator(clock, config.health.workerStaleSeconds),
                  new HealthCloudEventFactory(config, mapper, clock),
                  healthPublisher,
                  lifecycle,
                  clock);
      lifecycle.register(watcher, reporter, healthPublisher, filePublisher, uploader);
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  new Runnable() {
                    @Override
                    public void run() {
                      lifecycle.close();
                    }
                  },
                  "application-shutdown-hook"));
      if (reporter != null) reporter.start();
      watcher.start();
    } catch (Exception e) {
      lifecycle.terminate("STARTUP_FAILED", e.getMessage(), e);
    }
  }

  private static HealthKafkaPublisher initializeHealthPublisher(
      ApplicationConfig config,
      Properties properties,
      ObjectMapper mapper,
      ApplicationLifecycleManager lifecycle) {
    if (!config.health.enabled) return null;
    RuntimeException last = null;
    for (int attempt = 1; attempt <= config.health.initMaxAttempts; attempt++) {
      try {
        return new HealthKafkaPublisher(properties, config.healthProducer.topic, mapper);
      } catch (RuntimeException e) {
        last = e;
        LOG.warn(
            "Health producer initialization failed: attempt {}/{}",
            attempt,
            config.health.initMaxAttempts,
            e);
        if (attempt < config.health.initMaxAttempts) sleep(config.health.initBackoffMs);
      }
    }
    if (config.health.required)
      lifecycle.terminate(
          "HEALTH_KAFKA_INITIALIZATION_FAILED", "Health producer initialization failed", last);
    else
      LOG.error(
          "Health checks disabled after initialization failure because health.required=false",
          last);
    return null;
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
