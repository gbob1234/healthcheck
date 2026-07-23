package com.kafka.producer.image;

import com.kafka.producer.config.ApplicationConfig;
import com.kafka.producer.health.ApplicationHealthState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Watches ENTRY_CREATE and delegates stability checking and encoding to bounded worker threads. */
public final class ImageDirectoryWatcher implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ImageDirectoryWatcher.class);
    private final ApplicationConfig.ImageWatcher config;
    private final ImageFileStabilityChecker stability;
    private final ImageEncodingService encoding;
    private final ImageMessageFactory messages;
    private final ImageKafkaPublisher publisher;
    private final ApplicationHealthState state;
    private final ExecutorService workers;
    private final Set<Path> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private WatchService watchService;

    public ImageDirectoryWatcher(ApplicationConfig.ImageWatcher config, ImageFileStabilityChecker stability,
                                 ImageEncodingService encoding, ImageMessageFactory messages,
                                 ImageKafkaPublisher publisher, ApplicationHealthState state) {
        this.config = config;
        this.stability = stability;
        this.encoding = encoding;
        this.messages = messages;
        this.publisher = publisher;
        this.state = state;
        this.workers = Executors.newFixedThreadPool(config.threadCount);
    }

    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) return;
        watchService = FileSystems.getDefault().newWatchService();
        config.directory.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
        Thread thread = new Thread(new Runnable() { @Override public void run() { watchLoop(); } }, "image-watch-service");
        thread.setDaemon(false);
        thread.start();
        LOG.info("Watching image directory: {}", config.directory);
        try (java.util.stream.Stream<Path> paths = Files.list(config.directory)) {
            paths.filter(Files::isRegularFile).forEach(this::submitIfSupported);
        }
    }

    private void watchLoop() {
        try {
            while (running.get()) {
                WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
                state.workerProgressed();
                if (key == null) continue; // Idle input is normal; this tick proves the watcher thread is alive.
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) continue;
                    Path path = config.directory.resolve((Path) event.context()).toAbsolutePath().normalize();
                    if (path.startsWith(config.directory)) submitIfSupported(path);
                }
                if (!key.reset()) throw new IOException("Image directory WatchKey became invalid");
            }
        } catch (ClosedWatchServiceException ignored) {
            // Expected during normal or fatal shutdown.
        } catch (Exception e) {
            if (running.get()) {
                state.recordError("IMAGE_WATCHER_FAILED", e.getMessage());
                LOG.error("Image watcher stopped unexpectedly", e);
            }
        } finally { running.set(false); }
    }

    public boolean isSupported(Path path) {
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".tmp") || name.endsWith(".part") || name.startsWith("~")) return false;
        int dot = name.lastIndexOf('.');
        return dot >= 0 && config.extensions.contains(name.substring(dot + 1));
    }

    public boolean submitIfSupported(final Path path) {
        if (!running.get() || !isSupported(path) || !inFlight.add(path)) return false;
        state.imageDetected(inFlight.size());
        try {
            workers.submit(new Runnable() { @Override public void run() { process(path); } });
            return true;
        } catch (RuntimeException e) {
            inFlight.remove(path);
            state.imageProcessingFailed("IMAGE_EXECUTOR_REJECTED", e.getMessage(), inFlight.size());
            return false;
        }
    }

    private void process(Path path) {
        state.imageProcessingStarted();
        try {
            if (!stability.awaitStable(path)) throw new IOException("Image did not become stable before timeout");
            String base64 = encoding.encode(path);
            publisher.publish(path, state.nextImageKey(), messages.create(path, base64));
            state.imageProcessingSucceeded(inFlight.size() - 1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            state.imageProcessingFailed("IMAGE_PROCESSING_INTERRUPTED", e.getMessage(), inFlight.size() - 1);
        } catch (Exception e) {
            state.imageProcessingFailed("IMAGE_PROCESSING_FAILED", e.getMessage(), inFlight.size() - 1);
            LOG.warn("Image processing failed: {}", path, e);
        } finally { inFlight.remove(path); }
    }

    public boolean isRunning() { return running.get(); }

    public void close() {
        running.set(false);
        if (watchService != null) try { watchService.close(); } catch (IOException e) { LOG.warn("WatchService close failed", e); }
        workers.shutdown();
        try { if (!workers.awaitTermination(5, TimeUnit.SECONDS)) workers.shutdownNow(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); workers.shutdownNow(); }
    }
}
