package com.kafka.producer.image;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Waits for repeated equal non-zero sizes and verifies the file can be opened before processing. */
public final class ImageFileStabilityChecker {
    private final long intervalMs, timeoutMs, maxBytes;
    private final int requiredCount;

    public ImageFileStabilityChecker(long intervalMs, int requiredCount, long timeoutMs, long maxBytes) {
        this.intervalMs = intervalMs;
        this.requiredCount = requiredCount;
        this.timeoutMs = timeoutMs;
        this.maxBytes = maxBytes;
    }

    public boolean awaitStable(Path path) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long previous = -1;
        int stable = 0;
        while (System.currentTimeMillis() <= deadline) {
            try {
                if (!Files.isRegularFile(path)) return false;
                long size = Files.size(path);
                if (size > maxBytes) return false;
                stable = size > 0 && size == previous ? stable + 1 : 0;
                previous = size;
                if (stable >= requiredCount && canOpen(path)) return true;
            } catch (IOException ignored) {
                stable = 0; // Windows writers may temporarily lock a new file.
            }
            Thread.sleep(intervalMs);
        }
        return false;
    }

    private boolean canOpen(Path path) {
        try (InputStream stream = Files.newInputStream(path)) { return stream.read(new byte[0]) == 0; }
        catch (IOException e) { return false; }
    }
}
