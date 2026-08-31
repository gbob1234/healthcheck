package com.kafka.producer.kafka;

import com.kafka.producer.config.ApplicationConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/** Builds a fresh producer configuration so image and health producers share no mutable resource. */
public final class KafkaPropertiesFactory {
    public Properties create(ApplicationConfig.KafkaCommon common, ApplicationConfig.Producer producer) {
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, common.bootstrapServers);
        p.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producer.clientId);
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.ACKS_CONFIG, producer.acks);
        p.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(producer.retries));
        p.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(producer.requestTimeoutMs));
        p.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(producer.deliveryTimeoutMs));
        p.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(producer.maxBlockMs));
        p.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(producer.lingerMs));
        p.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(producer.bufferMemory));
        p.setProperty("security.protocol", common.securityMode.name());
        applySecurity(p, common);
        return p;
    }

    /** Applies only settings needed by the selected mode; secret values must never be logged. */
    private void applySecurity(Properties p, ApplicationConfig.KafkaCommon c) {
        if (c.securityMode == ApplicationConfig.SecurityMode.PLAINTEXT) return;
        setIfNotEmpty(p, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, c.truststoreLocation);
        setIfNotEmpty(p, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, c.truststorePassword);
        p.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, c.endpointIdentificationAlgorithm);
        setIfNotEmpty(p, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, c.keystoreLocation);
        setIfNotEmpty(p, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, c.keystorePassword);
        setIfNotEmpty(p, SslConfigs.SSL_KEY_PASSWORD_CONFIG, c.keyPassword);
        if (c.securityMode == ApplicationConfig.SecurityMode.SASL_SSL) {
            p.setProperty(SaslConfigs.SASL_MECHANISM, c.saslMechanism);
            p.setProperty(SaslConfigs.SASL_JAAS_CONFIG, c.saslJaasConfig.isEmpty() ? createJaas(c.saslUsername, c.saslPassword) : c.saslJaasConfig);
        }
    }

    private void setIfNotEmpty(Properties properties, String key, String value) {
        if (value != null && !value.isEmpty()) properties.setProperty(key, value);
    }

    public String createJaas(String username, String password) {
        return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + escape(username)
                + "\" password=\"" + escape(password) + "\";";
    }

    private String escape(String value) { return value.replace("\\", "\\\\").replace("\"", "\\\""); }
}
