package com.kafka.producer.file;

import java.nio.file.Path;

/** Builds the configured fixed-prefix key; same filenames intentionally overwrite. */
public final class ObjectKeyFactory {
  private final String prefix;

  public ObjectKeyFactory(String prefix) {
    this.prefix = prefix;
  }

  public String create(Path file) {
    String name = file.getFileName().toString();
    return prefix.isEmpty() ? name : prefix + "/" + name;
  }
}
