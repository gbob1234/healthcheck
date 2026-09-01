package com.kafka.producer.health;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.TestConfigFactory;
import com.kafka.producer.config.ApplicationConfig;
import com.kafka.producer.lifecycle.FatalFailureHandler;
import io.cloudevents.CloudEvent;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HealthReporterTest {
  @TempDir Path temp;

  @Test
  void isIdempotentAndCallsFatalOnlyAfterCountAndDurationThresholds() throws Exception {
    Properties p = TestConfigFactory.base(temp);
    p.setProperty("health.initial.delay.seconds", "100");
    p.setProperty("health.send.max.consecutive.failures", "1");
    p.setProperty("health.send.max.failure.duration.seconds", "1");
    p.setProperty("health.recovery.max.attempts", "1");
    p.setProperty("health.recovery.retry.backoff.ms", "0");
    ApplicationConfig config = TestConfigFactory.load(temp, p);
    MutableClock clock = new MutableClock(Instant.EPOCH);
    ApplicationHealthState state = new ApplicationHealthState(clock);
    CountDownLatch fatal = new CountDownLatch(1);
    HealthPublisher publisher =
        new HealthPublisher() {
          public void publish(String key, CloudEvent event, ResultCallback callback) {
            callback.complete(new RuntimeException("offline"));
          }

          public boolean reinitialize() {
            return false;
          }

          public void close() {}
        };
    FatalFailureHandler handler = (code, message, cause) -> fatal.countDown();
    HealthReporter reporter =
        new HealthReporter(
            config,
            state,
            new HealthStatusEvaluator(clock, 180),
            new HealthCloudEventFactory(config, new ObjectMapper(), clock),
            publisher,
            handler,
            clock);
    reporter.start();
    reporter.start();
    assertTrue(reporter.isRunning());
    clock.advanceSeconds(2);
    reporter.runOnce();
    assertTrue(fatal.await(2, TimeUnit.SECONDS));
    reporter.stop();
    reporter.stop();
    assertFalse(reporter.isRunning());
  }

  private static final class MutableClock extends Clock {
    private Instant instant;

    MutableClock(Instant instant) {
      this.instant = instant;
    }

    void advanceSeconds(long seconds) {
      instant = instant.plusSeconds(seconds);
    }

    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    public Clock withZone(ZoneId zone) {
      return this;
    }

    public Instant instant() {
      return instant;
    }
  }
}
