package com.kafka.producer.file;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.config.ApplicationConfig;
import com.kafka.producer.health.ApplicationHealthState;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileCollectionServicesTest {
  @TempDir Path temp;

  @Test
  void calculatesChecksumKeyAndDeterministicMetadata() throws Exception {
    Path file = temp.resolve("sample.csv");
    Files.write(file, "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8));
    String checksum = new FileChecksum().sha256(file);
    assertEquals(64, checksum.length());
    assertEquals("incoming/sample.csv", new ObjectKeyFactory("incoming").create(file));

    ApplicationConfig.Identity identity =
        new ApplicationConfig.Identity(
            "DEVICE-1",
            "EQUIPMENT-A",
            "SYSTEM",
            "WINDOWS_PC",
            "file-collector",
            "2.0",
            "health",
            "/health",
            "urn:health");
    FileMetadataFactory factory =
        new FileMetadataFactory(
            new ObjectMapper(),
            identity,
            Clock.fixed(Instant.parse("2026-08-31T01:02:03Z"), ZoneOffset.UTC));
    S3FileUploader.UploadResult upload =
        new S3FileUploader.UploadResult("bucket", "incoming/sample.csv", "etag", null);
    FileMetadataFactory.Metadata first = factory.create(file, Files.size(file), checksum, upload);
    FileMetadataFactory.Metadata second = factory.create(file, Files.size(file), checksum, upload);
    assertEquals(first.eventId, second.eventId);
    JsonNode json = new ObjectMapper().readTree(first.json);
    assertEquals("EQUIPMENT-A", json.get("deviceName").asText());
    assertEquals("CSV", json.get("fileType").asText());
    assertEquals("bucket", json.get("bucket").asText());
    assertEquals("incoming/sample.csv", json.get("objectKey").asText());
    assertFalse(json.has("endpoint"));
  }

  @Test
  void calculatesBase64Sha256ForOnlyTheRequestedMultipartRange() throws Exception {
    byte[] content = "0123456789".getBytes(StandardCharsets.UTF_8);
    Path file = temp.resolve("multipart.bin");
    Files.write(file, content);
    byte[] part = Arrays.copyOfRange(content, 2, 7);
    String expected =
        Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(part));

    assertEquals(expected, AwsS3FileUploader.sha256Base64(file, 2, 5));
  }

  @Test
  void uploadsPublishesThenMovesToOld() throws Exception {
    Path watch = Files.createDirectory(temp.resolve("watch"));
    Path archive = Files.createDirectory(watch.resolve("old"));
    Path file = watch.resolve("sample.csv");
    Files.write(file, "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8));
    List<String> calls = new ArrayList<String>();
    S3FileUploader uploader =
        new S3FileUploader() {
          public UploadResult upload(Path source, String key, String checksum) {
            calls.add("s3");
            return new UploadResult("bucket", key, "etag", null);
          }

          public void close() {}
        };
    MetadataPublisher publisher =
        new MetadataPublisher() {
          public void publish(String key, String json) {
            calls.add("kafka");
          }

          public void close() {}
        };
    CountDownLatch fatal = new CountDownLatch(1);
    FileDirectoryWatcher watcher = watcher(watch, archive, uploader, publisher, fatal);
    try {
      watcher.start();
      awaitFile(archive.resolve("sample.csv"));
      assertEquals(Arrays.asList("s3", "kafka"), calls);
      assertFalse(Files.exists(file));
      assertEquals(1, fatal.getCount());
    } finally {
      watcher.close();
    }
  }

  @Test
  void exhaustsS3RetriesThenReportsFatalWithoutPublishingOrMoving() throws Exception {
    Path watch = Files.createDirectory(temp.resolve("failed-watch"));
    Path archive = Files.createDirectory(watch.resolve("old"));
    Path file = watch.resolve("sample.jpg");
    Files.write(file, "image".getBytes(StandardCharsets.UTF_8));
    final int[] uploads = {0}, publishes = {0};
    S3FileUploader uploader =
        new S3FileUploader() {
          public UploadResult upload(Path source, String key, String checksum) throws Exception {
            uploads[0]++;
            throw new Exception("offline");
          }

          public void close() {}
        };
    MetadataPublisher publisher =
        new MetadataPublisher() {
          public void publish(String key, String json) {
            publishes[0]++;
          }

          public void close() {}
        };
    CountDownLatch fatal = new CountDownLatch(1);
    FileDirectoryWatcher watcher = watcher(watch, archive, uploader, publisher, fatal);
    try {
      watcher.start();
      assertTrue(fatal.await(3, TimeUnit.SECONDS));
      assertEquals(2, uploads[0]);
      assertEquals(0, publishes[0]);
      assertTrue(Files.exists(file));
      assertFalse(Files.exists(archive.resolve("sample.jpg")));
    } finally {
      watcher.close();
    }
  }

  @Test
  void deviceLocalDateDirectoryCreatedAfterStartupIsScannedAndArchivedBelowIt() throws Exception {
    Path root = Files.createDirectory(temp.resolve("daily-root"));
    List<String> calls = new ArrayList<String>();
    S3FileUploader uploader =
        new S3FileUploader() {
          public UploadResult upload(Path source, String key, String checksum) {
            calls.add("s3");
            return new UploadResult("bucket", key, "etag", null);
          }

          public void close() {}
        };
    MetadataPublisher publisher =
        new MetadataPublisher() {
          public void publish(String key, String json) {
            calls.add("kafka");
          }

          public void close() {}
        };
    CountDownLatch fatal = new CountDownLatch(1);
    Clock deviceClock = Clock.fixed(Instant.parse("2026-08-31T15:30:00Z"), ZoneId.of("Asia/Seoul"));
    ApplicationConfig.FileCollector config =
        new ApplicationConfig.FileCollector(
            root,
            "yyyyMMdd",
            "old",
            new HashSet<String>(Arrays.asList("csv", "jpg")),
            1,
            1,
            1000,
            1,
            1024 * 1024,
            2,
            0);
    ApplicationConfig.Identity identity =
        new ApplicationConfig.Identity(
            "DEVICE-1",
            "EQUIPMENT-A",
            "SYSTEM",
            "WINDOWS_PC",
            "file-collector",
            "2.0",
            "health",
            "/health",
            "urn:health");
    ApplicationHealthState state = new ApplicationHealthState(deviceClock);
    FileDirectoryWatcher watcher =
        new FileDirectoryWatcher(
            config,
            new FileStabilityChecker(1, 1, 1000, 1024 * 1024),
            new FileChecksum(),
            new ObjectKeyFactory("prefix"),
            uploader,
            new FileMetadataFactory(new ObjectMapper(), identity, deviceClock),
            publisher,
            state,
            (code, message, cause) -> fatal.countDown(),
            deviceClock);
    try {
      watcher.start();
      Path dateDirectory = Files.createDirectory(root.resolve("20260901"));
      Path file = dateDirectory.resolve("sample.csv");
      Files.write(file, "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8));

      awaitFile(dateDirectory.resolve("old").resolve("sample.csv"));
      assertEquals(Arrays.asList("s3", "kafka"), calls);
      assertFalse(Files.exists(file));
      assertEquals(1, fatal.getCount());
    } finally {
      watcher.close();
    }
  }

  private FileDirectoryWatcher watcher(
      Path watch,
      Path archive,
      S3FileUploader uploader,
      MetadataPublisher publisher,
      CountDownLatch fatal) {
    ApplicationConfig.FileCollector config =
        new ApplicationConfig.FileCollector(
            watch,
            archive,
            new HashSet<String>(Arrays.asList("csv", "jpg")),
            1,
            1,
            1000,
            1,
            1024 * 1024,
            2,
            0);
    ApplicationConfig.Identity identity =
        new ApplicationConfig.Identity(
            "DEVICE-1",
            "EQUIPMENT-A",
            "SYSTEM",
            "WINDOWS_PC",
            "file-collector",
            "2.0",
            "health",
            "/health",
            "urn:health");
    ApplicationHealthState state = new ApplicationHealthState(Clock.systemUTC());
    return new FileDirectoryWatcher(
        config,
        new FileStabilityChecker(1, 1, 1000, 1024 * 1024),
        new FileChecksum(),
        new ObjectKeyFactory("prefix"),
        uploader,
        new FileMetadataFactory(new ObjectMapper(), identity, Clock.systemUTC()),
        publisher,
        state,
        (code, message, cause) -> fatal.countDown(),
        Clock.systemUTC());
  }

  private static void awaitFile(Path path) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!Files.exists(path) && System.nanoTime() < deadline) Thread.sleep(10);
    assertTrue(Files.exists(path), "archive file was not created");
  }
}
