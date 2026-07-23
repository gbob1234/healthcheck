package com.kafka.producer.health;

import io.cloudevents.CloudEvent;

/** Testable boundary around asynchronous health delivery and producer replacement. */
public interface HealthPublisher extends AutoCloseable {
    interface ResultCallback { void complete(Exception error); }
    void publish(String key, CloudEvent event, ResultCallback callback);
    boolean reinitialize();
    void close();
}
