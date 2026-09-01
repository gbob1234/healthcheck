package com.kafka.producer.file;

import java.nio.file.Path;

public interface S3FileUploader extends AutoCloseable {
  UploadResult upload(Path file, String objectKey, String sha256) throws Exception;

  @Override
  void close();

  final class UploadResult {
    public final String bucket, objectKey, eTag, versionId;

    public UploadResult(String bucket, String objectKey, String eTag, String versionId) {
      this.bucket = bucket;
      this.objectKey = objectKey;
      this.eTag = eTag;
      this.versionId = versionId;
    }
  }
}
