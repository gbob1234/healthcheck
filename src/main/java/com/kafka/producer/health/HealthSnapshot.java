package com.kafka.producer.health;

import java.time.Instant;

/** Immutable point-in-time view used to create one internally consistent heartbeat. */
public final class HealthSnapshot {
    public final Instant programStartedAt, lastImageDetectedAt, lastImageProcessingStartedAt;
    public final Instant lastImageProcessingSuccessAt, lastImageProcessingFailureAt, lastImageSendAttemptAt;
    public final Instant lastImageSendSuccessAt, lastImageSendFailureAt, lastWorkerProgressAt;
    public final Instant lastHealthSendSuccessAt, lastHealthSendFailureAt, lastErrorAt;
    public final long imageDetectedCount, imageProcessingSuccessCount, imageProcessingFailureCount;
    public final long imageSendSuccessCount, imageSendFailureCount, heartbeatSequence;
    public final int pendingQueueSize, consecutiveHealthFailures;
    public final String lastErrorCode, lastErrorMessage;

    HealthSnapshot(Instant programStartedAt, Instant lastImageDetectedAt, Instant lastImageProcessingStartedAt,
                   Instant lastImageProcessingSuccessAt, Instant lastImageProcessingFailureAt,
                   Instant lastImageSendAttemptAt, Instant lastImageSendSuccessAt, Instant lastImageSendFailureAt,
                   Instant lastWorkerProgressAt, Instant lastHealthSendSuccessAt, Instant lastHealthSendFailureAt,
                   Instant lastErrorAt, long imageDetectedCount, long imageProcessingSuccessCount,
                   long imageProcessingFailureCount, long imageSendSuccessCount, long imageSendFailureCount,
                   int pendingQueueSize, String lastErrorCode, String lastErrorMessage,
                   long heartbeatSequence, int consecutiveHealthFailures) {
        this.programStartedAt = programStartedAt;
        this.lastImageDetectedAt = lastImageDetectedAt;
        this.lastImageProcessingStartedAt = lastImageProcessingStartedAt;
        this.lastImageProcessingSuccessAt = lastImageProcessingSuccessAt;
        this.lastImageProcessingFailureAt = lastImageProcessingFailureAt;
        this.lastImageSendAttemptAt = lastImageSendAttemptAt;
        this.lastImageSendSuccessAt = lastImageSendSuccessAt;
        this.lastImageSendFailureAt = lastImageSendFailureAt;
        this.lastWorkerProgressAt = lastWorkerProgressAt;
        this.lastHealthSendSuccessAt = lastHealthSendSuccessAt;
        this.lastHealthSendFailureAt = lastHealthSendFailureAt;
        this.lastErrorAt = lastErrorAt;
        this.imageDetectedCount = imageDetectedCount;
        this.imageProcessingSuccessCount = imageProcessingSuccessCount;
        this.imageProcessingFailureCount = imageProcessingFailureCount;
        this.imageSendSuccessCount = imageSendSuccessCount;
        this.imageSendFailureCount = imageSendFailureCount;
        this.pendingQueueSize = pendingQueueSize;
        this.lastErrorCode = lastErrorCode;
        this.lastErrorMessage = lastErrorMessage;
        this.heartbeatSequence = heartbeatSequence;
        this.consecutiveHealthFailures = consecutiveHealthFailures;
    }
}
