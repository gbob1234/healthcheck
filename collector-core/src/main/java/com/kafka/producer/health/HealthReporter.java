package com.kafka.producer.health;

import com.kafka.producer.config.ApplicationConfig;
import com.kafka.producer.lifecycle.FatalFailureHandler;
import io.cloudevents.CloudEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Schedules async heartbeats and applies the count AND duration recovery/fatal policy. */
public final class HealthReporter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HealthReporter.class);
    private final ApplicationConfig config;
    private final ApplicationHealthState state;
    private final HealthStatusEvaluator evaluator;
    private final HealthCloudEventFactory events;
    private final HealthPublisher publisher;
    private final FatalFailureHandler fatal;
    private final Clock clock;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private final Instant startedAt;

    public HealthReporter(ApplicationConfig config, ApplicationHealthState state,
                          HealthStatusEvaluator evaluator, HealthCloudEventFactory events,
                          HealthPublisher publisher, FatalFailureHandler fatal, Clock clock) {
        this.config = config; this.state = state; this.evaluator = evaluator; this.events = events;
        this.publisher = publisher; this.fatal = fatal; this.clock = clock; this.startedAt = Instant.now(clock);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        scheduler.scheduleWithFixedDelay(new Runnable() { @Override public void run() { runOnce(); } },
                config.health.initialDelaySeconds, config.health.intervalSeconds, TimeUnit.SECONDS);
        LOG.info("HealthReporter started: interval={}s", config.health.intervalSeconds);
    }

    public void runOnce() {
        if (!running.get()) return;
        try {
            final HealthSnapshot snapshot = state.snapshot();
            HealthStatus status = evaluator.evaluate(snapshot);
            long sequence = state.nextHeartbeatSequence();
            CloudEvent event = events.create(snapshot, status, sequence);
            publisher.publish(config.identity.deviceId, event, new HealthPublisher.ResultCallback() {
                @Override public void complete(Exception error) {
                    if (error == null) {
                        state.healthSendSucceeded();
                        LOG.debug("Health heartbeat sent");
                    } else {
                        state.healthSendFailed(error.getMessage());
                        LOG.warn("Health heartbeat send failed", error);
                        scheduleRecoveryIfUnrecoverable();
                    }
                }
            });
        } catch (Exception e) {
            state.healthSendFailed(e.getMessage());
            LOG.warn("Health heartbeat construction failed", e);
            scheduleRecoveryIfUnrecoverable();
        }
    }

    private void scheduleRecoveryIfUnrecoverable() {
        HealthSnapshot s = state.snapshot();
        Instant baseline = s.lastHealthSendSuccessAt == null ? startedAt : s.lastHealthSendSuccessAt;
        long failedSeconds = Math.max(0, Duration.between(baseline, Instant.now(clock)).getSeconds());
        if (s.consecutiveHealthFailures < config.health.maxConsecutiveFailures
                || failedSeconds < config.health.maxFailureDurationSeconds
                || !recovering.compareAndSet(false, true)) return;
        try { scheduler.execute(new Runnable() { @Override public void run() { recover(); } }); }
        catch (RuntimeException e) { recovering.set(false); }
    }

    /** Reinitializes in the scheduler thread so Kafka callback threads never close their own producer. */
    private void recover() {
        try {
            if (config.health.recoveryEnabled) {
                for (int attempt = 1; attempt <= config.health.recoveryMaxAttempts && running.get(); attempt++) {
                    LOG.warn("Reinitializing health producer: attempt {}/{}", attempt, config.health.recoveryMaxAttempts);
                    if (publisher.reinitialize()) {
                        state.resetHealthFailures();
                        recovering.set(false);
                        runOnce(); // test heartbeat after successful producer construction
                        return;
                    }
                    sleep(config.health.recoveryBackoffMs);
                }
            }
            state.recordError("HEALTH_KAFKA_UNRECOVERABLE", "Health producer recovery failed");
            fatal.terminate("HEALTH_KAFKA_UNRECOVERABLE", "Health producer recovery failed", null);
        } finally { recovering.set(false); }
    }

    private void sleep(long millis) { try { Thread.sleep(millis); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
    public boolean isRunning() { return running.get(); }
    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        scheduler.shutdownNow();
        try { scheduler.awaitTermination(config.health.shutdownSeconds, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        LOG.info("HealthReporter stopped");
    }
    public void close() { stop(); }
}
