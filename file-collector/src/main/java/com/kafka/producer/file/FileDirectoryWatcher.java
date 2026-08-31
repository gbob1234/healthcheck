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
import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
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
    private final Clock clock;
    private final DateTimeFormatter dateDirectoryFormatter;
    private final ExecutorService workers;
    private final Set<Path> inFlight = ConcurrentHashMap.newKeySet();
    private final Set<Path> registeredDirectories = ConcurrentHashMap.newKeySet();
    private final Map<WatchKey, Path> watchedDirectories = new ConcurrentHashMap<WatchKey, Path>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private WatchService watchService;

    public FileDirectoryWatcher(ApplicationConfig.FileCollector config, FileStabilityChecker stability,
                                FileChecksum checksums, ObjectKeyFactory keys, S3FileUploader uploader,
                                FileMetadataFactory messages, MetadataPublisher publisher,
                                ApplicationHealthState state, FatalFailureHandler fatal, Clock clock) {
        this.config = config; this.stability = stability; this.checksums = checksums; this.keys = keys;
        this.uploader = uploader; this.messages = messages; this.publisher = publisher;
        this.state = state; this.fatal = fatal; this.clock = clock;
        this.dateDirectoryFormatter = config.datedDirectoryMode
                ? DateTimeFormatter.ofPattern(config.dateDirectoryPattern, Locale.ROOT) : null;
        this.workers = Executors.newFixedThreadPool(config.threadCount);
    }
    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) return;
        watchService = FileSystems.getDefault().newWatchService();
        if (config.datedDirectoryMode) {
            registerDirectory(config.directory);
            ensureCurrentDateDirectory();
        } else {
            registerAndScanDirectory(config.directory);
        }
        Thread thread = new Thread(new Runnable() { @Override public void run() { watchLoop(); } }, "file-watch-service");
        thread.setDaemon(false); thread.start();
        LOG.info("Watching file directory: root={}, mode={}, datePattern={}, archive={}",
                config.directory, config.datedDirectoryMode ? "DATED" : "FIXED",
                config.dateDirectoryPattern,
                config.datedDirectoryMode ? config.archiveDirectoryName : config.archiveDirectory);
    }
    private void watchLoop() {
        try {
            while (running.get()) {
                WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
                state.workerProgressed();
                if (config.datedDirectoryMode) ensureCurrentDateDirectory();
                if (key == null) continue;
                Path watchedDirectory = watchedDirectories.get(key);
                if (watchedDirectory == null) { key.reset(); continue; }
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                        if (config.datedDirectoryMode && watchedDirectory.equals(config.directory))
                            ensureCurrentDateDirectory();
                        else scanDirectory(watchedDirectory);
                        continue;
                    }
                    Path path = watchedDirectory.resolve((Path) event.context()).toAbsolutePath().normalize();
                    if (!path.startsWith(watchedDirectory)) continue;
                    if (config.datedDirectoryMode && watchedDirectory.equals(config.directory)) {
                        if (path.equals(currentDateDirectory()) && Files.isDirectory(path))
                            registerAndScanDirectory(path);
                    } else {
                        submitIfSupported(path);
                    }
                }
                if (!key.reset()) {
                    watchedDirectories.remove(key);
                    registeredDirectories.remove(watchedDirectory);
                    if (watchedDirectory.equals(config.directory))
                        throw new IOException("File directory WatchKey became invalid: " + watchedDirectory);
                    LOG.warn("Date directory is no longer watchable: {}", watchedDirectory);
                }
            }
        } catch (ClosedWatchServiceException ignored) {
        } catch (Exception e) {
            if (running.get()) {
                state.recordError("FILE_WATCHER_FAILED", e.getMessage());
                fatal.terminate("FILE_WATCHER_FAILED", "File watcher stopped unexpectedly", e);
            }
        } finally { running.set(false); }
    }
    private void registerDirectory(Path directory) throws IOException {
        Path normalized = directory.toAbsolutePath().normalize();
        if (!registeredDirectories.add(normalized)) return;
        try {
            WatchKey key = normalized.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            watchedDirectories.put(key, normalized);
            LOG.info("Registered watch directory: {}", normalized);
        } catch (IOException | RuntimeException e) {
            registeredDirectories.remove(normalized);
            throw e;
        }
    }
    private void registerAndScanDirectory(Path directory) throws IOException {
        boolean alreadyRegistered = registeredDirectories.contains(directory.toAbsolutePath().normalize());
        registerDirectory(directory);
        if (!alreadyRegistered) scanDirectory(directory);
    }
    private void ensureCurrentDateDirectory() throws IOException {
        Path current = currentDateDirectory();
        if (Files.isDirectory(current)) registerAndScanDirectory(current);
    }
    private Path currentDateDirectory() {
        return config.directory.resolve(dateDirectoryFormatter.format(LocalDate.now(clock)))
                .toAbsolutePath().normalize();
    }
    private void scanDirectory(Path directory) throws IOException {
        try (java.util.stream.Stream<Path> paths = Files.list(directory)) {
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
            Path archiveDirectory = archiveDirectory(path);
            Files.move(path, archiveDirectory.resolve(path.getFileName()), StandardCopyOption.REPLACE_EXISTING);
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
    private Path archiveDirectory(Path source) throws IOException {
        if (!config.datedDirectoryMode) return config.archiveDirectory;
        Path parent = source.getParent().toAbsolutePath().normalize();
        Path archive = parent.resolve(config.archiveDirectoryName).normalize();
        if (!archive.getParent().equals(parent)) throw new IOException("Archive directory escapes date directory");
        Files.createDirectories(archive);
        if (!Files.isDirectory(archive) || !Files.isWritable(archive))
            throw new IOException("Archive directory is not writable: " + archive);
        return archive;
    }
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
