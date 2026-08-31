package com.kafka.producer.file;

public interface MetadataPublisher extends AutoCloseable {
    void publish(String key, String json) throws Exception;
    @Override void close();
}
