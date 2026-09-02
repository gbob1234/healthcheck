package com.kafka.producer.config;

import static org.junit.jupiter.api.Assertions.*;

import com.kafka.producer.TestConfigFactory;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigLoaderTest {
  @TempDir Path temp;

  @Test
  void loadsPlaintextAndWindowsStylePath() throws Exception {
    ApplicationConfig c = TestConfigFactory.load(temp, TestConfigFactory.base(temp));
    assertEquals(ApplicationConfig.SecurityMode.PLAINTEXT, c.kafka.securityMode);
    assertEquals(temp.toAbsolutePath().normalize(), c.fileCollector.directory);
    assertEquals(
        temp.resolve("old").toAbsolutePath().normalize(), c.fileCollector.archiveDirectory);
    assertTrue(c.fileCollector.extensions.contains("csv"));
    assertEquals("equipment-collection", c.s3.bucket);
    assertEquals("production/equipment", c.s3.objectKeyPrefix);
    assertTrue(c.s3.pathStyleAccessEnabled);
    assertFalse(c.s3.tlsVerify);
    assertEquals("DEVICE-001", c.identity.deviceId);
    assertEquals("DEVICE-001-file", c.fileProducer.clientId);
    assertEquals("DEVICE-001-health", c.healthProducer.clientId);
    assertEquals(
        "/systems/EQUIPMENT_FILE_COLLECTOR/devices/DEVICE-001/programs/file-collector",
        c.identity.eventSource);
  }

  @Test
  void loadsSslAndSaslSsl() throws Exception {
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
    sasl.setProperty("kafka.sasl.username", "user");
    sasl.setProperty("kafka.sasl.password", "pass");
    assertEquals(
        ApplicationConfig.SecurityMode.SASL_SSL,
        TestConfigFactory.load(temp, sasl).kafka.securityMode);
  }

  @Test
  void loadsTargetTemplateWithSuffixAndRejectsInvalidPatterns() throws Exception {
    Properties dated = TestConfigFactory.base(temp);
    dated.remove("file.watch.directory");
    dated.remove("file.archive.directory");
    dated.setProperty("target.base.dir", temp.resolve("{yyyyMMdd}").resolve("EQP01").toString());
    dated.setProperty("file.archive.directory.name", "old");
    ApplicationConfig config = TestConfigFactory.load(temp, dated);
    assertTrue(config.fileCollector.datedDirectoryMode);
    assertEquals(temp.toAbsolutePath().normalize(), config.fileCollector.directory);
    assertEquals(
        temp.resolve("{yyyyMMdd}").resolve("EQP01").toAbsolutePath().normalize().toString(),
        config.fileCollector.targetDirectoryTemplate);
    assertEquals("yyyyMMdd", config.fileCollector.dateDirectoryPattern);
    assertEquals("old", config.fileCollector.archiveDirectoryName);
    assertNull(config.fileCollector.archiveDirectory);

    dated.setProperty("target.base.dir", temp.resolve("{YYYYMMDD}").toString());
    assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, dated));

    dated.setProperty("target.base.dir", temp.resolve("{yyyyMMdd").toString());
    assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, dated));
  }

  @Test
  void loadsFixedTargetBaseDirectoryAndRejectsLegacyDirectoryCombination() throws Exception {
    Properties fixed = TestConfigFactory.base(temp);
    fixed.remove("file.watch.directory");
    fixed.remove("file.archive.directory");
    fixed.setProperty("target.base.dir", temp.toString());

    ApplicationConfig config = TestConfigFactory.load(temp, fixed);
    assertFalse(config.fileCollector.datedDirectoryMode);
    assertEquals(temp.toAbsolutePath().normalize(), config.fileCollector.directory);
    assertEquals(
        temp.resolve("old").toAbsolutePath().normalize(), config.fileCollector.archiveDirectory);

    fixed.setProperty("file.watch.directory", temp.toString());
    assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, fixed));
  }

  @Test
  void rejectsMissingAndInvalidSecurityMode() throws Exception {
    Properties missing = TestConfigFactory.base(temp);
    missing.remove("kafka.bootstrap.servers");
    assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, missing));
    Properties invalid = TestConfigFactory.base(temp);
    invalid.setProperty("kafka.security.mode", "MAGIC");
    assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, invalid));
    Properties missingDevice = TestConfigFactory.base(temp);
    missingDevice.remove("device.id");
    assertThrows(IllegalArgumentException.class, () -> TestConfigFactory.load(temp, missingDevice));
    Properties invalidTruststore = TestConfigFactory.base(temp);
    invalidTruststore.setProperty("kafka.security.mode", "SSL");
    invalidTruststore.setProperty(
        "kafka.ssl.truststore.location", temp.resolve("missing.jks").toString());
    assertThrows(
        IllegalArgumentException.class, () -> TestConfigFactory.load(temp, invalidTruststore));
  }
}
