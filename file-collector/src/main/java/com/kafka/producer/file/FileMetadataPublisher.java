package com.kafka.producer.file;

import com.kafka.producer.health.ApplicationHealthState;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Synchronously awaits broker acknowledgement so archiving cannot precede Kafka delivery. */
public final class FileMetadataPublisher implements MetadataPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(FileMetadataPublisher.class);
  private final KafkaProducer<String, String> producer;
  private final String topic;
  private final ApplicationHealthState state;

  public FileMetadataPublisher(Properties properties, String topic, ApplicationHealthState state) {
    this(new KafkaProducer<String, String>(properties), topic, state);
  }

  FileMetadataPublisher(
      KafkaProducer<String, String> producer, String topic, ApplicationHealthState state) {
    this.producer = producer;
    this.topic = topic;
    this.state = state;
  }

  public void publish(String key, String json) throws Exception {
    state.fileSendAttempted();
    try {
      RecordMetadata metadata =
          producer.send(new ProducerRecord<String, String>(topic, key, json)).get();
      state.fileSendSucceeded();
      LOG.debug(
          "File metadata sent: topic={}, partition={}, offset={}",
          metadata.topic(),
          metadata.partition(),
          metadata.offset());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      state.fileSendFailed("FILE_KAFKA_SEND_INTERRUPTED", e.getMessage());
      throw e;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      state.fileSendFailed("FILE_KAFKA_SEND_FAILED", cause.getMessage());
      if (cause instanceof Exception) throw (Exception) cause;
      throw e;
    } catch (RuntimeException e) {
      state.fileSendFailed("FILE_KAFKA_SEND_FAILED", e.getMessage());
      throw e;
    }
  }

  public void close() {
    producer.close(Duration.ofSeconds(5));
  }
}
