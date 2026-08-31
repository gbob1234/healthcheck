package com.kafka.producer.health;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class ApplicationHealthStateTest {
    @Test void snapshotsCountersSequenceAndTruncatedErrorConcurrently() throws Exception {
        ApplicationHealthState state = new ApplicationHealthState(Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        int threads = 4, iterations = 100;
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) new Thread(() -> { for (int i = 0; i < iterations; i++) state.fileDetected(i); done.countDown(); }).start();
        done.await();
        state.recordError("CODE", new String(new char[700]).replace('\0', 'x'));
        assertEquals(400, state.snapshot().fileDetectedCount);
        assertEquals(512, state.snapshot().lastErrorMessage.length());
        assertEquals(1, state.nextHeartbeatSequence());
        assertEquals(1, state.snapshot().heartbeatSequence);
    }
}
