package com.kafka.producer.file;

import com.kafka.producer.config.ApplicationConfig;
import com.kafka.producer.health.ApplicationHealthState;
import com.kafka.producer.lifecycle.FatalFailureHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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

/** Watches configured extensions and performs S3, Kafka, then archive in that exact order. */
public final class FileDirectoryWatcher implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FileDirectoryWatcher.class);
    private final ApplicationConfig.FileCollector config;
    private final FileStabilityChecker stability;
    private final FileChecksum checksums;
    private final ObjectKeyFactory keys;
    private final S3FileUploader uploader;
    private final FileMetadataFactory messages;
    private final MetadataPublisher publisher;
    private final ApplicationHealthState state;
    private final FatalFailureHandler fatal;
    private final ExecutorService workers;
    private final Set<Path> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private WatchService watchService;

    public FileDirectoryWatcher(ApplicationConfig.FileCollector config, FileStabilityChecker stability,
                                FileChecksum checksums, ObjectKeyFactory keys, S3FileUploader uploader,
                                FileMetadataFactory messages, MetadataPublisher publisher,
                                ApplicationHealthState state, FatalFailureHandler fatal) {
        this.config = config; this.stability = stability; this.checksums = checksums; this.keys = keys;
        this.uploader = uploader; this.messages = messages; this.publisher = publisher;
        this.state = state; this.fatal = fatal;
        this.workers = Executors.newFixedThreadPool(config.threadCount);
    }
    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) return;
        watchService = FileSystems.getDefault().newWatchService();
        config.directory.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
        Thread thread = new Thread(new Runnable() { @Override public void run() { watchLoop(); } }, "file-watch-service");
        thread.setDaemon(false); thread.start();
        LOG.info("Watching file directory: {}", config.directory);
        scanDirectory();
    }
    private void watchLoop() {
        try {
            while (running.get()) {
                WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
                state.workerProgressed();
                if (key == null) continue;
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) { scanDirectory(); continue; }
                    Path path = config.directory.resolve((Path) event.context()).toAbsolutePath().normalize();
                    if (path.startsWith(config.directory)) submitIfSupported(path);
                }
                if (!key.reset()) throw new IOException("File directory WatchKey became invalid");
            }
        } catch (ClosedWatchServiceException ignored) {
        } catch (Exception e) {
            if (running.get()) {
                state.recordError("FILE_WATCHER_FAILED", e.getMessage());
                fatal.terminate("FILE_WATCHER_FAILED", "File watcher stopped unexpectedly", e);
            }
        } finally { running.set(false); }
    }
    private void scanDirectory() throws IOException {
        try (java.util.stream.Stream<Path> paths = Files.list(config.directory)) {
            paths.filter(Files::isRegularFile).forEach(this::submitIfSupported);
        }
    }
    public boolean isSupported(Path path) {
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".tmp") || name.endsWith(".part") || name.startsWith("~")) return false;
        int dot = name.lastIndexOf('.');
        return dot >= 0 && config.extensions.contains(name.substring(dot + 1));
    }
    public boolean submitIfSupported(final Path path) {
        if (!running.get() || !isSupported(path) || !inFlight.add(path)) return false;
        state.fileDetected(inFlight.size());
        try {
            workers.submit(new Runnable() { @Override public void run() { process(path); } });
            return true;
        } catch (RuntimeException e) {
            inFlight.remove(path);
            state.fileProcessingFailed("FILE_EXECUTOR_REJECTED", e.getMessage(), inFlight.size());
            return false;
        }
    }
    private void process(Path path) {
        state.fileProcessingStarted();
        try {
            if (!stability.awaitStable(path)) {
                state.fileProcessingFailed("FILE_STABILITY_FAILED", "File did not become stable before timeout", inFlight.size() - 1);
                LOG.warn("File did not become stable and remains in the watch directory: {}", path);
                return;
            }
            long size = Files.size(path);
            String checksum = checksums.sha256(path);
            String objectKey = keys.create(path);
            S3FileUploader.UploadResult uploaded = retryUpload(path, objectKey, checksum);
            FileMetadataFactory.Metadata metadata = messages.create(path, size, checksum, uploaded);
            retryPublish(metadata);
            Files.move(path, config.archiveDirectory.resolve(path.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            state.fileProcessingSucceeded(inFlight.size() - 1);
            LOG.info("File collected and archived: file={}, bucket={}, objectKey={}", path.getFileName(), uploaded.bucket, uploaded.objectKey);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failFatal("FILE_PROCESSING_INTERRUPTED", path, e);
        } catch (StageFailure e) {
            failFatal(e.code, path, e.getCause() == null ? e : e.getCause());
        } catch (Exception e) {
            failFatal("FILE_ARCHIVE_FAILED", path, e);
        } finally { inFlight.remove(path); }
    }
    private S3FileUploader.UploadResult retryUpload(Path path, String key, String checksum) throws StageFailure, InterruptedException {
        Exception last = null;
        for (int attempt = 1; attempt <= config.maxAttempts; attempt++) {
            try { return uploader.upload(path, key, checksum); }
            catch (Exception e) { last = e; LOG.warn("S3 upload failed: attempt {}/{}, file={}", attempt, config.maxAttempts, path, e); if (attempt < config.maxAttempts) sleepBackoff(); }
        }
        throw new StageFailure("S3_UPLOAD_FAILED", last);
    }
    private void retryPublish(FileMetadataFactory.Metadata metadata) throws StageFailure, InterruptedException {
        Exception last = null;
        for (int attempt = 1; attempt <= config.maxAttempts; attempt++) {
            try { publisher.publish(metadata.eventId, metadata.json); return; }
            catch (InterruptedException e) { throw e; }
            catch (Exception e) { last = e; LOG.warn("Kafka metadata send failed: attempt {}/{}", attempt, config.maxAttempts, e); if (attempt < config.maxAttempts) sleepBackoff(); }
        }
        throw new StageFailure("FILE_KAFKA_SEND_FAILED", last);
    }
    private void sleepBackoff() throws InterruptedException { if (config.retryBackoffMs > 0) Thread.sleep(config.retryBackoffMs); }
    private void failFatal(String code, Path path, Throwable error) {
        String message = "File collection failed: " + path + ": " + (error == null ? "unknown" : error.getMessage());
        state.fileProcessingFailed(code, message, inFlight.size() - 1);
        fatal.terminate(code, message, error);
    }
    public boolean isRunning() { return running.get(); }
    public void close() {
        running.set(false);
        if (watchService != null) try { watchService.close(); } catch (IOException e) { LOG.warn("WatchService close failed", e); }
        workers.shutdown();
        try { if (!workers.awaitTermination(5, TimeUnit.SECONDS)) workers.shutdownNow(); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); workers.shutdownNow(); }
    }
    private static final class StageFailure extends Exception {
        private static final long serialVersionUID = 1L;
        private final String code;
        StageFailure(String code, Throwable cause) { super(code, cause); this.code = code; }
    }
}
