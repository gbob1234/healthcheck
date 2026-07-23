package com.kafka.producer.image;

import com.kafka.producer.health.ApplicationHealthState;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;

/** Owns only the image KafkaProducer and updates image delivery state from async callbacks. */
public final class ImageKafkaPublisher implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ImageKafkaPublisher.class);
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final ApplicationHealthState state;

    public ImageKafkaPublisher(Properties properties, String topic, ApplicationHealthState state) {
        this(new KafkaProducer<String, String>(properties), topic, state);
    }

    ImageKafkaPublisher(KafkaProducer<String, String> producer, String topic, ApplicationHealthState state) {
        this.producer = producer;
        this.topic = topic;
        this.state = state;
    }

    public void publish(final Path path, String key, String json) {
        state.imageSendAttempted();
        producer.send(new ProducerRecord<String, String>(topic, key, json), new Callback() {
            @Override public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    state.imageSendSucceeded();
                    try { Files.deleteIfExists(path); }
                    catch (Exception e) { LOG.warn("Image was sent but could not be deleted: {}", path, e); }
                    LOG.debug("Image sent: topic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    state.imageSendFailed("IMAGE_KAFKA_SEND_FAILED", exception.getMessage());
                    LOG.warn("Image send failed: {}", path, exception);
                }
            }
        });
    }

    public void close() { producer.close(Duration.ofSeconds(5)); }
}
