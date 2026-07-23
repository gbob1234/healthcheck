package com.kafka.producer.health;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Thread-safe state using atomic fields so Kafka callbacks and workers never need a broad lock. */
public final class ApplicationHealthState {
    private static final int MAX_ERROR_LENGTH = 512;
    private final Clock clock;
    private final Instant startedAt;
    private final AtomicReference<Instant> detected = new AtomicReference<Instant>();
    private final AtomicReference<Instant> processingStarted = new AtomicReference<Instant>();
    private final AtomicReference<Instant> processingSuccess = new AtomicReference<Instant>();
    private final AtomicReference<Instant> processingFailure = new AtomicReference<Instant>();
    private final AtomicReference<Instant> imageSendAttempt = new AtomicReference<Instant>();
    private final AtomicReference<Instant> imageSendSuccess = new AtomicReference<Instant>();
    private final AtomicReference<Instant> imageSendFailure = new AtomicReference<Instant>();
    private final AtomicReference<Instant> workerProgress = new AtomicReference<Instant>();
    private final AtomicReference<Instant> healthSendSuccess = new AtomicReference<Instant>();
    private final AtomicReference<Instant> healthSendFailure = new AtomicReference<Instant>();
    private final AtomicReference<Instant> lastErrorAt = new AtomicReference<Instant>();
    private final AtomicReference<String> lastErrorCode = new AtomicReference<String>();
    private final AtomicReference<String> lastErrorMessage = new AtomicReference<String>();
    private final AtomicLong detectedCount = new AtomicLong();
    private final AtomicLong processingSuccessCount = new AtomicLong();
    private final AtomicLong processingFailureCount = new AtomicLong();
    private final AtomicLong imageSendSuccessCount = new AtomicLong();
    private final AtomicLong imageSendFailureCount = new AtomicLong();
    private final AtomicLong heartbeatSequence = new AtomicLong();
    private final AtomicLong imageKeySequence = new AtomicLong();
    private final AtomicInteger pending = new AtomicInteger();
    private final AtomicInteger consecutiveHealthFailures = new AtomicInteger();

    public ApplicationHealthState(Clock clock) { this.clock = clock; this.startedAt = Instant.now(clock); }
    private Instant now() { return Instant.now(clock); }
    public void workerProgressed() { workerProgress.set(now()); }
    public void imageDetected(int pendingSize) { detected.set(now()); detectedCount.incrementAndGet(); pending.set(Math.max(0, pendingSize)); }
    public void imageProcessingStarted() { processingStarted.set(now()); workerProgressed(); }
    public void imageProcessingSucceeded(int pendingSize) { processingSuccess.set(now()); processingSuccessCount.incrementAndGet(); pending.set(Math.max(0, pendingSize)); workerProgressed(); }
    public void imageProcessingFailed(String code, String message, int pendingSize) { processingFailure.set(now()); processingFailureCount.incrementAndGet(); pending.set(Math.max(0, pendingSize)); recordError(code, message); workerProgressed(); }
    public void imageSendAttempted() { imageSendAttempt.set(now()); }
    public void imageSendSucceeded() { imageSendSuccess.set(now()); imageSendSuccessCount.incrementAndGet(); }
    public void imageSendFailed(String code, String message) { imageSendFailure.set(now()); imageSendFailureCount.incrementAndGet(); recordError(code, message); }
    public void healthSendSucceeded() { healthSendSuccess.set(now()); consecutiveHealthFailures.set(0); }
    public void healthSendFailed(String message) { healthSendFailure.set(now()); consecutiveHealthFailures.incrementAndGet(); recordError("HEALTH_KAFKA_SEND_FAILED", message); }
    public void resetHealthFailures() { consecutiveHealthFailures.set(0); }
    public void recordError(String code, String message) { lastErrorAt.set(now()); lastErrorCode.set(code); lastErrorMessage.set(truncate(message)); }
    public long nextHeartbeatSequence() { return heartbeatSequence.incrementAndGet(); }
    public String nextImageKey() { return DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").withZone(ZoneOffset.UTC).format(now()) + "-" + imageKeySequence.incrementAndGet(); }

    public HealthSnapshot snapshot() {
        return new HealthSnapshot(startedAt, detected.get(), processingStarted.get(), processingSuccess.get(),
                processingFailure.get(), imageSendAttempt.get(), imageSendSuccess.get(), imageSendFailure.get(),
                workerProgress.get(), healthSendSuccess.get(), healthSendFailure.get(), lastErrorAt.get(),
                detectedCount.get(), processingSuccessCount.get(), processingFailureCount.get(),
                imageSendSuccessCount.get(), imageSendFailureCount.get(), pending.get(), lastErrorCode.get(),
                lastErrorMessage.get(), heartbeatSequence.get(), consecutiveHealthFailures.get());
    }

    private String truncate(String value) {
        if (value == null) return null;
        String clean = value.replace('\n', ' ').replace('\r', ' ');
        return clean.length() <= MAX_ERROR_LENGTH ? clean : clean.substring(0, MAX_ERROR_LENGTH);
    }
}
