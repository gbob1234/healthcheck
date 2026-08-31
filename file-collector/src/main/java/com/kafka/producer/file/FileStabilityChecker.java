package com.kafka.producer.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Waits for repeated equal non-zero sizes and verifies that the file can be opened. */
public final class FileStabilityChecker {
    private final long intervalMs, timeoutMs, maxBytes;
    private final int requiredCount;

    public FileStabilityChecker(long intervalMs, int requiredCount, long timeoutMs, long maxBytes) {
        this.intervalMs = intervalMs; this.requiredCount = requiredCount;
        this.timeoutMs = timeoutMs; this.maxBytes = maxBytes;
    }
    public boolean awaitStable(Path path) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs, previous = -1;
        int stable = 0;
        while (System.currentTimeMillis() <= deadline) {
            try {
                if (!Files.isRegularFile(path)) return false;
                long size = Files.size(path);
                if (size > maxBytes) return false;
                stable = size > 0 && size == previous ? stable + 1 : 0;
                previous = size;
                if (stable >= requiredCount && canOpen(path)) return true;
            } catch (IOException ignored) { stable = 0; }
            Thread.sleep(intervalMs);
        }
        return false;
    }
    private boolean canOpen(Path path) {
        try (InputStream stream = Files.newInputStream(path)) { return stream.read(new byte[0]) == 0; }
        catch (IOException e) { return false; }
    }
}
