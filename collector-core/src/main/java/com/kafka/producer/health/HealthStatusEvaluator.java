package com.kafka.producer.health;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Distinguishes an idle file source from a stale watcher and evaluates actual processing failures.
 */
public final class HealthStatusEvaluator {
  private final Clock clock;
  private final long workerStaleSeconds;

  public HealthStatusEvaluator(Clock clock, long workerStaleSeconds) {
    this.clock = clock;
    this.workerStaleSeconds = workerStaleSeconds;
  }

  public HealthStatus evaluate(HealthSnapshot s) {
    Instant now = Instant.now(clock);
    if (s.consecutiveHealthFailures > 0)
      return new HealthStatus(
          HealthStatus.Level.WARN,
          "HEALTH_KAFKA_SEND_FAILED",
          "Health delivery has recent failures");
    if (s.lastFileSendFailureAt != null && newer(s.lastFileSendFailureAt, s.lastFileSendSuccessAt))
      return new HealthStatus(
          HealthStatus.Level.WARN,
          "FILE_KAFKA_SEND_FAILED",
          "Latest file metadata delivery failed");
    if (s.lastFileProcessingFailureAt != null
        && newer(s.lastFileProcessingFailureAt, s.lastFileProcessingSuccessAt))
      return new HealthStatus(
          HealthStatus.Level.WARN, "FILE_PROCESSING_FAILED", "Latest file processing failed");
    if (s.lastWorkerProgressAt != null
        && Duration.between(s.lastWorkerProgressAt, now).getSeconds() > workerStaleSeconds)
      return new HealthStatus(
          HealthStatus.Level.WARN, "FILE_WATCHER_STALE", "File watcher has not reported progress");
    if (s.heartbeatSequence == 0)
      return new HealthStatus(HealthStatus.Level.UNKNOWN, "INSUFFICIENT_DATA", "First heartbeat");
    return new HealthStatus(HealthStatus.Level.UP, "NORMAL", "Kafka producer is running normally");
  }

  private boolean newer(Instant candidate, Instant other) {
    return other == null || candidate.isAfter(other);
  }
}
