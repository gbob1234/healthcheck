package com.kafka.producer.lifecycle;

import com.kafka.producer.health.HealthKafkaPublisher;
import com.kafka.producer.health.HealthReporter;
import com.kafka.producer.image.ImageDirectoryWatcher;
import com.kafka.producer.image.ImageKafkaPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/** Centralizes ordered, idempotent cleanup; this is the sole System.exit call site. */
public final class ApplicationLifecycleManager implements FatalFailureHandler, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationLifecycleManager.class);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final int exitCode;
    private volatile ImageDirectoryWatcher watcher;
    private volatile HealthReporter reporter;
    private volatile HealthKafkaPublisher healthPublisher;
    private volatile ImageKafkaPublisher imagePublisher;

    public ApplicationLifecycleManager(int exitCode) { this.exitCode = exitCode; }
    public void register(ImageDirectoryWatcher watcher, HealthReporter reporter,
                         HealthKafkaPublisher healthPublisher, ImageKafkaPublisher imagePublisher) {
        this.watcher = watcher; this.reporter = reporter; this.healthPublisher = healthPublisher; this.imagePublisher = imagePublisher;
    }

    public void terminate(final String errorCode, final String message, final Throwable cause) {
        if (!closing.compareAndSet(false, true)) return;
        LOG.error("Fatal application failure: code={}, message={}", errorCode, message, cause);
        Thread shutdown = new Thread(new Runnable() {
            @Override public void run() { cleanup(); System.exit(exitCode); }
        }, "fatal-shutdown");
        shutdown.setDaemon(false);
        shutdown.start();
    }

    public void close() {
        if (!closing.compareAndSet(false, true)) return;
        cleanup();
        LOG.info("Application stopped normally");
    }

    private void cleanup() {
        closeQuietly(watcher, "image watcher");
        closeQuietly(reporter, "health reporter");
        closeQuietly(healthPublisher, "health producer");
        closeQuietly(imagePublisher, "image producer");
    }
    private void closeQuietly(AutoCloseable resource, String name) {
        if (resource == null) return;
        try { resource.close(); } catch (Exception e) { LOG.warn("Failed to close {}", name, e); }
    }
}
