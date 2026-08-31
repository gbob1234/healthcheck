package com.kafka.producer.file;

import com.kafka.producer.config.ApplicationConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.utils.AttributeMap;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;

/** AWS SDK v2 uploader using PutObject for small files and multipart upload for large files. */
public final class AwsS3FileUploader implements S3FileUploader {
    private final ApplicationConfig.S3 config;
    private final S3Client client;
    private final SdkHttpClient httpClient;

    public AwsS3FileUploader(ApplicationConfig.S3 config) {
        this.config = config;
        this.httpClient = UrlConnectionHttpClient.builder().buildWithDefaults(AttributeMap.builder()
                .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, !config.tlsVerify).build());
        this.client = S3Client.builder().endpointOverride(config.endpoint).region(Region.of(config.region))
                .credentialsProvider(credentials(config)).httpClient(httpClient)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(config.pathStyleAccessEnabled).build()).build();
    }
    AwsS3FileUploader(ApplicationConfig.S3 config, S3Client client) {
        this.config = config; this.client = client; this.httpClient = null;
    }

    public UploadResult upload(Path file, String objectKey, String sha256) throws IOException {
        long size = Files.size(file);
        if (size < config.multipartThresholdBytes) return put(file, objectKey, sha256);
        return multipart(file, objectKey, size);
    }
    private UploadResult put(Path file, String key, String sha256) throws IOException {
        PutObjectRequest request = PutObjectRequest.builder().bucket(config.bucket).key(key)
                .contentType(contentType(file)).checksumSHA256(hexSha256ToBase64(sha256)).build();
        PutObjectResponse response = client.putObject(request, RequestBody.fromFile(file));
        return new UploadResult(config.bucket, key, response.eTag(), response.versionId());
    }
    private UploadResult multipart(Path file, String key, long size) throws IOException {
        CreateMultipartUploadResponse created = client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(config.bucket).key(key).contentType(contentType(file))
                .checksumAlgorithm(ChecksumAlgorithm.SHA256).build());
        String uploadId = created.uploadId();
        List<CompletedPart> parts = new ArrayList<CompletedPart>();
        try {
            long offset = 0;
            int partNumber = 1;
            while (offset < size) {
                if (partNumber > 10000) throw new IOException("Multipart upload exceeds S3's 10,000 part limit");
                long length = Math.min(config.multipartPartSizeBytes, size - offset);
                String partChecksumSha256 = sha256Base64(file, offset, length);
                UploadPartRequest request = UploadPartRequest.builder().bucket(config.bucket).key(key)
                        .uploadId(uploadId).partNumber(partNumber).contentLength(length)
                        .checksumSHA256(partChecksumSha256).build();
                UploadPartResponse response = client.uploadPart(request,
                        RequestBody.fromContentProvider(new RangedFileProvider(file, offset, length), length, "application/octet-stream"));
                parts.add(CompletedPart.builder().partNumber(partNumber).eTag(response.eTag())
                        .checksumSHA256(response.checksumSHA256() == null
                                ? partChecksumSha256 : response.checksumSHA256()).build());
                offset += length;
                partNumber++;
            }
            CompleteMultipartUploadResponse response = client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(config.bucket).key(key).uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build()).build());
            return new UploadResult(config.bucket, key, response.eTag(), response.versionId());
        } catch (RuntimeException | IOException e) {
            try { client.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(config.bucket).key(key).uploadId(uploadId).build()); }
            catch (RuntimeException abortError) { e.addSuppressed(abortError); }
            throw e;
        }
    }
    private static AwsCredentialsProvider credentials(ApplicationConfig.S3 config) {
        if (config.accessKey.isEmpty()) return DefaultCredentialsProvider.builder().build();
        if (!config.sessionToken.isEmpty()) return StaticCredentialsProvider.create(
                AwsSessionCredentials.create(config.accessKey, config.secretKey, config.sessionToken));
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKey, config.secretKey));
    }
    static String sha256Base64(Path file, long offset, long length) throws IOException {
        MessageDigest digest;
        try { digest = MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 is unavailable", e); }
        byte[] buffer = new byte[64 * 1024];
        try (InputStream in = new RangedFileProvider(file, offset, length).newStream()) {
            int read;
            while ((read = in.read(buffer)) != -1) digest.update(buffer, 0, read);
        }
        return Base64.getEncoder().encodeToString(digest.digest());
    }
    private static String hexSha256ToBase64(String hex) {
        if (hex == null || hex.length() != 64) throw new IllegalArgumentException("SHA-256 hex value must contain 64 characters");
        byte[] bytes = new byte[32];
        for (int i = 0; i < bytes.length; i++) {
            int high = Character.digit(hex.charAt(i * 2), 16);
            int low = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (high < 0 || low < 0) throw new IllegalArgumentException("SHA-256 contains a non-hex character");
            bytes[i] = (byte) ((high << 4) | low);
        }
        return Base64.getEncoder().encodeToString(bytes);
    }
    private static String contentType(Path file) {
        try { String detected = Files.probeContentType(file); if (detected != null) return detected; }
        catch (IOException ignored) { }
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".csv")) return "text/csv";
        if (name.endsWith(".jpg") || name.endsWith(".jpeg")) return "image/jpeg";
        if (name.endsWith(".png")) return "image/png";
        return "application/octet-stream";
    }
    public void close() {
        try { client.close(); }
        finally { if (httpClient != null) httpClient.close(); }
    }

    private static final class RangedFileProvider implements software.amazon.awssdk.http.ContentStreamProvider {
        private final Path file; private final long offset, length;
        RangedFileProvider(Path file, long offset, long length) { this.file = file; this.offset = offset; this.length = length; }
        public InputStream newStream() {
            try {
                InputStream in = Files.newInputStream(file);
                long remaining = offset;
                while (remaining > 0) { long skipped = in.skip(remaining); if (skipped <= 0) { in.close(); throw new IOException("Unable to seek multipart source"); } remaining -= skipped; }
                return new LimitedInputStream(in, length);
            } catch (IOException e) { throw new IllegalStateException("Unable to open multipart source", e); }
        }
    }
    private static final class LimitedInputStream extends InputStream {
        private final InputStream delegate; private long remaining;
        LimitedInputStream(InputStream delegate, long remaining) { this.delegate = delegate; this.remaining = remaining; }
        public int read() throws IOException { if (remaining == 0) return -1; int value = delegate.read(); if (value >= 0) remaining--; return value; }
        public int read(byte[] b, int off, int len) throws IOException { if (remaining == 0) return -1; int read = delegate.read(b, off, (int) Math.min(len, remaining)); if (read > 0) remaining -= read; return read; }
        public void close() throws IOException { delegate.close(); }
    }
}
