package com.kafka.producer;

import com.kafka.producer.config.ApplicationConfig;
import com.kafka.producer.config.ConfigLoader;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class TestConfigFactory {
    private TestConfigFactory() { }
    public static Properties base(Path watchDirectory) throws Exception {
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get("config.properties"))) { p.load(in); }
        p.setProperty("image.watch.directory", watchDirectory.toString());
        return p;
    }
    public static ApplicationConfig load(Path tempDirectory, Properties p) throws Exception {
        Path file = tempDirectory.resolve("test.properties");
        try (OutputStream out = Files.newOutputStream(file)) { p.store(out, "test"); }
        return new ConfigLoader().load(file);
    }
}
