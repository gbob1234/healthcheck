package com.kafka.producer.config;

import com.kafka.producer.TestConfigFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ConfigLoaderTest {
    @TempDir Path temp;

    @Test void loadsPlaintextAndWindowsStylePath() throws Exception {
        ApplicationConfig c = TestConfigFactory.load(temp, TestConfigFactory.base(temp));
        assertEquals(ApplicationConfig.SecurityMode.PLAINTEXT, c.kafka.securityMode);
        assertEquals(temp.toAbsolutePath().normalize(), c.imageWatcher.directory);
    }

    @Test void loadsSslAndSaslSsl() throws Exception {
        Path trust = Files.createFile(temp.resolve("trust.jks"));
        Properties ssl = TestConfigFactory.base(temp);
        ssl.setProperty("kafka.security.mode", "SSL"); ssl.setProperty("kafka.ssl.truststore.location", trust.toString());
        ssl.setProperty("kafka.ssl.truststore.password", "secret");
        assertEquals(ApplicationConfig.SecurityMode.SSL, TestConfigFactory.load(temp, ssl).kafka.securityMode);
        Properties sasl = TestConfigFactory.base(temp);
        sasl.setProperty("kafka.security.mode", "sasl_ssl"); sasl.setProperty("kafka.ssl.truststore.location", trust.toString());
        sasl.setProperty("kafka.ssl.truststore.password", "secret"); sasl.setProperty("kafka.sasl.username", "user"); sasl.setProperty("kafka.sasl.password", "pass");
        assertEquals(ApplicationConfig.SecurityMode.SASL_SSL, TestConfigFactory.load(temp, sasl).kafka.securityMode);
    }

    @Test void rejectsMissingAndInvalidSecurityMode() throws Exception {
        Properties missing = TestConfigFactory.base(temp); missing.remove("kafka.bootstrap.servers");
        assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, missing));
        Properties invalid = TestConfigFactory.base(temp); invalid.setProperty("kafka.security.mode", "MAGIC");
        assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, invalid));
    }
}
