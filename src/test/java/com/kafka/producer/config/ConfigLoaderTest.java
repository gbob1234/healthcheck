package com.kafka.producer.config;

import com.kafka.producer.TestConfigFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ConfigLoaderTest {
    @TempDir Path temp;

    @Test void loadsPlaintextAndWindowsStylePath() throws Exception {
        ApplicationConfig c = TestConfigFactory.load(temp, TestConfigFactory.base(temp));
        assertEquals(ApplicationConfig.SecurityMode.PLAINTEXT, c.kafka.securityMode);
        assertEquals(temp.toAbsolutePath().normalize(), c.imageWatcher.directory);
        assertEquals("DEVICE-001", c.identity.deviceId);
        assertEquals("DEVICE-001-image", c.imageProducer.clientId);
        assertEquals("DEVICE-001-health", c.healthProducer.clientId);
        assertEquals("/systems/EQUIPMENT_IMAGE_SENDER/devices/DEVICE-001/programs/image-producer",
                c.identity.eventSource);
    }

    @Test void loadsSslAndSaslSsl() throws Exception {
        Properties ssl = TestConfigFactory.base(temp);
        ssl.setProperty("kafka.security.mode", "SSL");
        ssl.setProperty("kafka.ssl.key.password", "secret");
        ApplicationConfig sslConfig = TestConfigFactory.load(temp, ssl);
        assertEquals(ApplicationConfig.SecurityMode.SSL, sslConfig.kafka.securityMode);
        assertEquals("", sslConfig.kafka.truststoreLocation);
        assertEquals("", sslConfig.kafka.keystoreLocation);
        assertEquals("secret", sslConfig.kafka.keyPassword);

        Properties sasl = TestConfigFactory.base(temp);
        sasl.setProperty("kafka.security.mode", "sasl_ssl");
        sasl.setProperty("kafka.ssl.key.password", "secret");
        sasl.setProperty("kafka.sasl.username", "user"); sasl.setProperty("kafka.sasl.password", "pass");
        assertEquals(ApplicationConfig.SecurityMode.SASL_SSL, TestConfigFactory.load(temp, sasl).kafka.securityMode);
    }

    @Test void rejectsMissingAndInvalidSecurityMode() throws Exception {
        Properties missing = TestConfigFactory.base(temp); missing.remove("kafka.bootstrap.servers");
        assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, missing));
        Properties invalid = TestConfigFactory.base(temp); invalid.setProperty("kafka.security.mode", "MAGIC");
        assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, invalid));
        Properties missingDevice = TestConfigFactory.base(temp); missingDevice.remove("device.id");
        assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, missingDevice));
        Properties invalidTruststore = TestConfigFactory.base(temp);
        invalidTruststore.setProperty("kafka.security.mode", "SSL");
        invalidTruststore.setProperty("kafka.ssl.truststore.location", temp.resolve("missing.jks").toString());
        assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, invalidTruststore));
    }
}
