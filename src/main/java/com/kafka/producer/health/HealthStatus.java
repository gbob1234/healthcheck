package com.kafka.producer.health;

/** Evaluated health level and stable machine-readable code. */
public final class HealthStatus {
    public enum Level { UP, WARN, DOWN, UNKNOWN }
    public final Level level;
    public final String code, message;
    public HealthStatus(Level level, String code, String message) { this.level = level; this.code = code; this.message = message; }
}
