package com.quiltdata.quiltcore.key;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import com.quiltdata.quiltcore.S3ClientStore;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3PhysicalKey extends PhysicalKey {
    private String bucket;
    private String key;
    private String versionId;

    public S3PhysicalKey(String bucket, String key, String versionId) {
        this.bucket = bucket;
        this.key = key;
        this.versionId = versionId;
    }

    public S3PhysicalKey(URI uri) {
        if (!uri.getScheme().equals("s3")) {
            throw new IllegalArgumentException("Unexpected URI scheme: " + uri.getScheme());
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Missing bucket");
        }
        if (uri.getPort() != -1) {
            throw new IllegalArgumentException("Unexpected port");
        }

        bucket = uri.getHost();
        key = uri.getPath().substring(1);  // Remove /
        versionId = null;

        if (uri.getQuery() != null) {
            for (String nameValuePair : uri.getQuery().split("&")) {
                String[] parts = nameValuePair.split("=", 2);
                if (parts.length == 2) {
                    String name = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
                    String value = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                    if (name.equals("versionId")) {
                        versionId = value;
                        break;
                    }
                }
            }
        }
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public String getVersionId() {
        return versionId;
    }

    @Override
    public byte[] getBytes() throws IOException {
        S3Client s3;
        try {
            s3 = S3ClientStore.getClient(bucket);
        } catch (NoSuchBucketException e) {
            throw new IOException("Bucket " + bucket + " does not exist", e);
        } catch (S3Exception e) {
            throw new IOException("Could not look up bucket " + bucket, e);
        }

        GetObjectRequest objectRequest = GetObjectRequest
            .builder()
            .bucket(bucket)
            .key(key)
            .versionId(versionId)
            .build();

        try {
            return s3.getObject(objectRequest).readAllBytes();
        } catch (S3Exception e) {
            throw new IOException("Could not read uri: " + toUri(), e);
        }
    }

    @Override
    public PhysicalKey resolve(String child) {
        if (versionId != null) {
            throw new IllegalArgumentException("Cannot append paths to URIs with version IDs");
        }
        return new S3PhysicalKey(bucket, joinPaths(key, child), null);
    }

    @Override
    public URI toUri() {
        String query = versionId == null ? null : "versionId=" + URLEncoder.encode(versionId, StandardCharsets.UTF_8);
        try {
            return new URI("s3", bucket, "/" + key, query, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Should not happen!", e);
        }
    }
}
