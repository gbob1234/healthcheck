package com.kafka.producer.health;

import java.time.Instant;

/** Immutable point-in-time view used to create one internally consistent heartbeat. */
public final class HealthSnapshot {
  public final Instant programStartedAt, lastFileDetectedAt, lastFileProcessingStartedAt;
  public final Instant lastFileProcessingSuccessAt, lastFileProcessingFailureAt;
  public final Instant lastFileSendAttemptAt, lastFileSendSuccessAt, lastFileSendFailureAt;
  public final Instant lastWorkerProgressAt,
      lastHealthSendSuccessAt,
      lastHealthSendFailureAt,
      lastErrorAt;
  public final long fileDetectedCount, fileProcessingSuccessCount, fileProcessingFailureCount;
  public final long fileSendSuccessCount, fileSendFailureCount, heartbeatSequence;
  public final int pendingQueueSize, consecutiveHealthFailures;
  public final String lastErrorCode, lastErrorMessage;

  HealthSnapshot(
      Instant programStartedAt,
      Instant lastFileDetectedAt,
      Instant lastFileProcessingStartedAt,
      Instant lastFileProcessingSuccessAt,
      Instant lastFileProcessingFailureAt,
      Instant lastFileSendAttemptAt,
      Instant lastFileSendSuccessAt,
      Instant lastFileSendFailureAt,
      Instant lastWorkerProgressAt,
      Instant lastHealthSendSuccessAt,
      Instant lastHealthSendFailureAt,
      Instant lastErrorAt,
      long fileDetectedCount,
      long fileProcessingSuccessCount,
      long fileProcessingFailureCount,
      long fileSendSuccessCount,
      long fileSendFailureCount,
      int pendingQueueSize,
      String lastErrorCode,
      String lastErrorMessage,
      long heartbeatSequence,
      int consecutiveHealthFailures) {
    this.programStartedAt = programStartedAt;
    this.lastFileDetectedAt = lastFileDetectedAt;
    this.lastFileProcessingStartedAt = lastFileProcessingStartedAt;
    this.lastFileProcessingSuccessAt = lastFileProcessingSuccessAt;
    this.lastFileProcessingFailureAt = lastFileProcessingFailureAt;
    this.lastFileSendAttemptAt = lastFileSendAttemptAt;
    this.lastFileSendSuccessAt = lastFileSendSuccessAt;
    this.lastFileSendFailureAt = lastFileSendFailureAt;
    this.lastWorkerProgressAt = lastWorkerProgressAt;
    this.lastHealthSendSuccessAt = lastHealthSendSuccessAt;
    this.lastHealthSendFailureAt = lastHealthSendFailureAt;
    this.lastErrorAt = lastErrorAt;
    this.fileDetectedCount = fileDetectedCount;
    this.fileProcessingSuccessCount = fileProcessingSuccessCount;
    this.fileProcessingFailureCount = fileProcessingFailureCount;
    this.fileSendSuccessCount = fileSendSuccessCount;
    this.fileSendFailureCount = fileSendFailureCount;
    this.pendingQueueSize = pendingQueueSize;
    this.lastErrorCode = lastErrorCode;
    this.lastErrorMessage = lastErrorMessage;
    this.heartbeatSequence = heartbeatSequence;
    this.consecutiveHealthFailures = consecutiveHealthFailures;
  }
}
