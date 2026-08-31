package com.kafka.producer.config;

import com.kafka.producer.TestConfigFactory;
import com.kafka.producer.kafka.KafkaPropertiesFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaPropertiesFactoryTest {
    @TempDir Path temp;
    @Test void createsEscapedJaasAndSeparateProperties() throws Exception {
        Properties input = TestConfigFactory.base(temp);
        input.setProperty("kafka.security.mode", "SASL_SSL");
        input.setProperty("kafka.ssl.key.password", "key-secret");
        input.setProperty("kafka.sasl.username", "u\"ser"); input.setProperty("kafka.sasl.password", "p\\ass");
        ApplicationConfig c = TestConfigFactory.load(temp, input);
        KafkaPropertiesFactory factory = new KafkaPropertiesFactory();
        Properties image = factory.create(c.kafka, c.fileProducer);
        Properties health = factory.create(c.kafka, c.healthProducer);
        assertNotSame(image, health);
        assertEquals("SASL_SSL", image.getProperty("security.protocol"));
        assertNull(image.getProperty("ssl.truststore.location"));
        assertNull(image.getProperty("ssl.keystore.location"));
        assertEquals("key-secret", image.getProperty("ssl.key.password"));
        assertTrue(image.getProperty("sasl.jaas.config").contains("u\\\"ser"));
        assertTrue(image.getProperty("sasl.jaas.config").contains("p\\\\ass"));
    }

    @Test void kafkaProducerAcceptsSslKeyPasswordWithoutStores() throws Exception {
        Properties input = TestConfigFactory.base(temp);
        input.setProperty("kafka.bootstrap.servers", "127.0.0.1:1");
        input.setProperty("kafka.security.mode", "SSL");
        input.setProperty("kafka.ssl.key.password", "key-secret");
        ApplicationConfig config = TestConfigFactory.load(temp, input);
        Properties properties = new KafkaPropertiesFactory().create(config.kafka, config.fileProducer);

        assertDoesNotThrow(() -> {
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
            producer.close(Duration.ZERO);
        });
    }
}
