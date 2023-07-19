package com.quiltdata.quiltcore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.nio.spi.s3.S3ClientStore;


public class Entry {
    public static enum HashType {
        SHA256,
    }

    public static class Hash {
        public final HashType type;
        public final String value;

        public Hash(HashType type, String value) {
            this.type = type;
            this.value = value;
        }
    }

    private URI physicalKey;
    private long size;
    private Hash hash;

    public Entry(URI physicalKey, long size, Hash hash) {
        if (physicalKey.getScheme().equals("file")) {
            if (physicalKey.getHost() != null) {
                throw new IllegalArgumentException("Bad file URL: " + physicalKey);
            }
        } else if (physicalKey.getScheme().equals("s3")) {
            if (physicalKey.getHost() == null) {
                throw new IllegalArgumentException("Bad s3 URL: " + physicalKey);
            }
        } else {
            throw new IllegalArgumentException("Unsupported URL scheme: " + physicalKey);
        }

        this.physicalKey = physicalKey;
        this.size = size;
        this.hash = hash;
    }

    public URI getPhysicalKey() {
        return physicalKey;
    }

    public long getSize() {
        return size;
    }

    public Hash getHash() {
        return hash;
    }

    public byte[] getBytes() throws URISyntaxException, IOException {
        // TODO: Verify the hash.

        if (physicalKey.getScheme().equals("s3")) {
            // Quilt S3 URIs are not the same as AWS or s3fs S3 URIs:
            // 1) The path is URL-encoded
            // 2) There is a versionId query parameter

            String bucket = physicalKey.getHost();
            String key = physicalKey.getPath().substring(1);  // Remove /
            String versionId = null;

            for (String nameValuePair : physicalKey.getQuery().split("&")) {
                String[] parts = nameValuePair.split("=");
                if (parts.length == 2) {
                    String name = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
                    String value = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                    if (name.equals("versionId")) {
                        versionId = value;
                    }
                }
            }

            S3Client s3 = S3ClientStore.getInstance().getClientForBucketName(bucket);

            GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .bucket(bucket)
                .key(key)
                .versionId(versionId)
                .build();

            try {
                ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
                return objectBytes.asByteArray();
            } catch (S3Exception e) {
                // Convert into IOException for consistency with file://
                throw new IOException("Could not read uri: " + physicalKey, e);
            }
        } else if (physicalKey.getScheme().equals("file")) {
            Path path = Path.of(physicalKey);
            return Files.readAllBytes(path);
        } else {
            throw new IOException("Unsupported URI: " + physicalKey);
        }
    }
}
