package com.kafka.producer.lifecycle;

/** Injectable boundary that keeps tests from invoking System.exit. */
public interface FatalFailureHandler {
    void terminate(String errorCode, String message, Throwable cause);
}
