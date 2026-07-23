package com.kafka.producer.config;

import com.kafka.producer.TestConfigFactory;
import com.kafka.producer.kafka.KafkaPropertiesFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaPropertiesFactoryTest {
    @TempDir Path temp;
    @Test void createsEscapedJaasAndSeparateProperties() throws Exception {
        Path trust = Files.createFile(temp.resolve("trust.jks"));
        Properties input = TestConfigFactory.base(temp);
        input.setProperty("kafka.security.mode", "SASL_SSL"); input.setProperty("kafka.ssl.truststore.location", trust.toString());
        input.setProperty("kafka.ssl.truststore.password", "secret"); input.setProperty("kafka.sasl.username", "u\"ser"); input.setProperty("kafka.sasl.password", "p\\ass");
        ApplicationConfig c = TestConfigFactory.load(temp, input);
        KafkaPropertiesFactory factory = new KafkaPropertiesFactory();
        Properties image = factory.create(c.kafka, c.imageProducer);
        Properties health = factory.create(c.kafka, c.healthProducer);
        assertNotSame(image, health);
        assertEquals("SASL_SSL", image.getProperty("security.protocol"));
        assertTrue(image.getProperty("sasl.jaas.config").contains("u\\\"ser"));
        assertTrue(image.getProperty("sasl.jaas.config").contains("p\\\\ass"));
    }
}
