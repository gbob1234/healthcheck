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
        Path config = Paths.get("config.properties");
        if (!Files.isRegularFile(config)) config = Paths.get("..", "config.properties");
        try (InputStream in = Files.newInputStream(config)) { p.load(in); }
        p.setProperty("file.watch.directory", watchDirectory.toString());
        p.setProperty("file.archive.directory", watchDirectory.resolve("old").toString());
        return p;
    }
    public static ApplicationConfig load(Path tempDirectory, Properties p) throws Exception {
        Path file = tempDirectory.resolve("test.properties");
        try (OutputStream out = Files.newOutputStream(file)) { p.store(out, "test"); }
        return new ConfigLoader().load(file);
    }
}
