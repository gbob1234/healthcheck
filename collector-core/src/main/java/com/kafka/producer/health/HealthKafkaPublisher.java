package com.kafka.producer.health;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Owns the dedicated health producer and supports serialized producer replacement during recovery.
 */
public final class HealthKafkaPublisher implements HealthPublisher {
  private final Properties properties;
  private final String topic;
  private final ObjectMapper mapper;
  private final AtomicReference<KafkaProducer<String, String>> producer =
      new AtomicReference<KafkaProducer<String, String>>();

  public HealthKafkaPublisher(Properties properties, String topic, ObjectMapper mapper) {
    this.properties = new Properties();
    this.properties.putAll(properties);
    this.topic = topic;
    this.mapper = mapper;
    producer.set(new KafkaProducer<String, String>(this.properties));
  }

  public void publish(String key, CloudEvent event, final HealthPublisher.ResultCallback callback) {
    try {
      String json = mapper.writeValueAsString(event);
      producer
          .get()
          .send(
              new ProducerRecord<String, String>(topic, key, json),
              new Callback() {
                @Override
                public void onCompletion(
                    org.apache.kafka.clients.producer.RecordMetadata metadata,
                    Exception exception) {
                  callback.complete(exception);
                }
              });
    } catch (Exception e) {
      callback.complete(e);
    }
  }

  public synchronized boolean reinitialize() {
    KafkaProducer<String, String> previous = producer.getAndSet(null);
    if (previous != null)
      try {
        previous.close(Duration.ofSeconds(3));
      } catch (Exception ignored) {
      }
    try {
      producer.set(new KafkaProducer<String, String>(properties));
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  public synchronized void close() {
    KafkaProducer<String, String> current = producer.getAndSet(null);
    if (current != null) current.close(Duration.ofSeconds(5));
  }
}
