package com.kafka.producer.health;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Thread-safe file collection and delivery state shared by workers and Kafka callbacks. */
public final class ApplicationHealthState {
    private static final int MAX_ERROR_LENGTH = 512;
    private final Clock clock;
    private final Instant startedAt;
    private final AtomicReference<Instant> detected = new AtomicReference<Instant>();
    private final AtomicReference<Instant> processingStarted = new AtomicReference<Instant>();
    private final AtomicReference<Instant> processingSuccess = new AtomicReference<Instant>();
    private final AtomicReference<Instant> processingFailure = new AtomicReference<Instant>();
    private final AtomicReference<Instant> fileSendAttempt = new AtomicReference<Instant>();
    private final AtomicReference<Instant> fileSendSuccess = new AtomicReference<Instant>();
    private final AtomicReference<Instant> fileSendFailure = new AtomicReference<Instant>();
    private final AtomicReference<Instant> workerProgress = new AtomicReference<Instant>();
    private final AtomicReference<Instant> healthSendSuccess = new AtomicReference<Instant>();
    private final AtomicReference<Instant> healthSendFailure = new AtomicReference<Instant>();
    private final AtomicReference<Instant> lastErrorAt = new AtomicReference<Instant>();
    private final AtomicReference<String> lastErrorCode = new AtomicReference<String>();
    private final AtomicReference<String> lastErrorMessage = new AtomicReference<String>();
    private final AtomicLong detectedCount = new AtomicLong(), processingSuccessCount = new AtomicLong();
    private final AtomicLong processingFailureCount = new AtomicLong(), fileSendSuccessCount = new AtomicLong();
    private final AtomicLong fileSendFailureCount = new AtomicLong(), heartbeatSequence = new AtomicLong();
    private final AtomicInteger pending = new AtomicInteger(), consecutiveHealthFailures = new AtomicInteger();

    public ApplicationHealthState(Clock clock) { this.clock = clock; this.startedAt = Instant.now(clock); }
    private Instant now() { return Instant.now(clock); }
    public void workerProgressed() { workerProgress.set(now()); }
    public void fileDetected(int pendingSize) { detected.set(now()); detectedCount.incrementAndGet(); pending.set(Math.max(0, pendingSize)); }
    public void fileProcessingStarted() { processingStarted.set(now()); workerProgressed(); }
    public void fileProcessingSucceeded(int pendingSize) { processingSuccess.set(now()); processingSuccessCount.incrementAndGet(); pending.set(Math.max(0, pendingSize)); workerProgressed(); }
    public void fileProcessingFailed(String code, String message, int pendingSize) { processingFailure.set(now()); processingFailureCount.incrementAndGet(); pending.set(Math.max(0, pendingSize)); recordError(code, message); workerProgressed(); }
    public void fileSendAttempted() { fileSendAttempt.set(now()); }
    public void fileSendSucceeded() { fileSendSuccess.set(now()); fileSendSuccessCount.incrementAndGet(); }
    public void fileSendFailed(String code, String message) { fileSendFailure.set(now()); fileSendFailureCount.incrementAndGet(); recordError(code, message); }
    public void healthSendSucceeded() { healthSendSuccess.set(now()); consecutiveHealthFailures.set(0); }
    public void healthSendFailed(String message) { healthSendFailure.set(now()); consecutiveHealthFailures.incrementAndGet(); recordError("HEALTH_KAFKA_SEND_FAILED", message); }
    public void resetHealthFailures() { consecutiveHealthFailures.set(0); }
    public void recordError(String code, String message) { lastErrorAt.set(now()); lastErrorCode.set(code); lastErrorMessage.set(truncate(message)); }
    public long nextHeartbeatSequence() { return heartbeatSequence.incrementAndGet(); }
    public HealthSnapshot snapshot() {
        return new HealthSnapshot(startedAt, detected.get(), processingStarted.get(), processingSuccess.get(),
                processingFailure.get(), fileSendAttempt.get(), fileSendSuccess.get(), fileSendFailure.get(),
                workerProgress.get(), healthSendSuccess.get(), healthSendFailure.get(), lastErrorAt.get(),
                detectedCount.get(), processingSuccessCount.get(), processingFailureCount.get(),
                fileSendSuccessCount.get(), fileSendFailureCount.get(), pending.get(), lastErrorCode.get(),
                lastErrorMessage.get(), heartbeatSequence.get(), consecutiveHealthFailures.get());
    }
    private String truncate(String value) {
        if (value == null) return null;
        String clean = value.replace('\n', ' ').replace('\r', ' ');
        return clean.length() <= MAX_ERROR_LENGTH ? clean : clean.substring(0, MAX_ERROR_LENGTH);
    }
}
