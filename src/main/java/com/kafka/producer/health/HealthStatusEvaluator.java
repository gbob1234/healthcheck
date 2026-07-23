package com.kafka.producer.health;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/** Distinguishes an idle image source from a stale watcher and evaluates actual processing failures. */
public final class HealthStatusEvaluator {
    private final Clock clock;
    private final long workerStaleSeconds;
    public HealthStatusEvaluator(Clock clock, long workerStaleSeconds) { this.clock = clock; this.workerStaleSeconds = workerStaleSeconds; }

    public HealthStatus evaluate(HealthSnapshot s) {
        Instant now = Instant.now(clock);
        if (s.consecutiveHealthFailures > 0) return new HealthStatus(HealthStatus.Level.WARN, "HEALTH_KAFKA_SEND_FAILED", "Health delivery has recent failures");
        if (s.lastImageSendFailureAt != null && newer(s.lastImageSendFailureAt, s.lastImageSendSuccessAt))
            return new HealthStatus(HealthStatus.Level.WARN, "IMAGE_KAFKA_SEND_FAILED", "Latest image delivery failed");
        if (s.lastImageProcessingFailureAt != null && newer(s.lastImageProcessingFailureAt, s.lastImageProcessingSuccessAt))
            return new HealthStatus(HealthStatus.Level.WARN, "IMAGE_PROCESSING_FAILED", "Latest image processing failed");
        if (s.lastWorkerProgressAt != null && Duration.between(s.lastWorkerProgressAt, now).getSeconds() > workerStaleSeconds)
            return new HealthStatus(HealthStatus.Level.WARN, "IMAGE_WATCHER_STALE", "Image watcher has not reported progress");
        if (s.heartbeatSequence == 0) return new HealthStatus(HealthStatus.Level.UNKNOWN, "INSUFFICIENT_DATA", "First heartbeat");
        return new HealthStatus(HealthStatus.Level.UP, "NORMAL", "Kafka producer is running normally");
    }
    private boolean newer(Instant candidate, Instant other) { return other == null || candidate.isAfter(other); }
}
