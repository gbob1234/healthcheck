package com.kafka.producer.image;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

/** Validates and streams an image into memory with an explicit upper bound before Base64 encoding. */
public final class ImageEncodingService {
    private final long maxBytes;

    public ImageEncodingService(long maxBytes) { this.maxBytes = maxBytes; }

    public String encode(Path path) throws IOException {
        long size = Files.size(path);
        if (size <= 0) throw new IOException("Image file is empty: " + path);
        if (size > maxBytes) throw new IOException("Image exceeds maximum size: " + size + " > " + maxBytes);
        ByteArrayOutputStream out = new ByteArrayOutputStream((int) Math.min(size, 1048576));
        byte[] buffer = new byte[8192];
        long total = 0;
        try (InputStream in = Files.newInputStream(path)) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                total += read;
                if (total > maxBytes) throw new IOException("Image grew beyond maximum size while reading");
                out.write(buffer, 0, read);
            }
        }
        if (total == 0) throw new IOException("Image file is empty: " + path);
        return Base64.getEncoder().encodeToString(out.toByteArray());
    }
}
