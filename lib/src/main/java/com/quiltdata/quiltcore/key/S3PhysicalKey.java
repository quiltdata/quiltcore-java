package com.quiltdata.quiltcore.key;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.stream.Stream;

import com.quiltdata.quiltcore.S3ClientStore;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3PhysicalKey extends PhysicalKey {
    private final String bucket;
    private final String key;
    private final String versionId;

    public S3PhysicalKey(String bucket, String key, String versionId) {
        this.bucket = bucket;
        this.key = key;
        this.versionId = versionId;
    }

    public S3PhysicalKey(URI uri) {
        if (uri.getScheme() == null || !uri.getScheme().equals("s3")) {
            throw new IllegalArgumentException("Unexpected URI scheme: " + uri.getScheme());
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Missing bucket");
        }
        if (uri.getPort() != -1) {
            throw new IllegalArgumentException("Unexpected port");
        }
        if (uri.getPath() == null || !uri.getPath().startsWith("/")) {
            throw new IllegalArgumentException("Unexpected path: " + uri.getPath());
        }

        bucket = uri.getHost();
        key = uri.getPath().substring(1);  // Remove /

        String versionId = null;
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
        this.versionId = versionId;
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

    private S3Client getClient() throws IOException {
        try {
            return S3ClientStore.getClient(bucket);
        } catch (NoSuchBucketException e) {
            throw new IOException("Bucket " + bucket + " does not exist", e);
        } catch (S3Exception e) {
            throw new IOException("Could not look up bucket " + bucket, e);
        }
    }

    private boolean looksLikeDir() {
        return key.length() == 0 || key.charAt(key.length() - 1) == '/';
    }

    private ResponseInputStream<GetObjectResponse> getObject() throws IOException {
        S3Client s3 = getClient();

        GetObjectRequest objectRequest = GetObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .versionId(versionId)
            .build();

        try {
            return s3.getObject(objectRequest);
        } catch (NoSuchKeyException e) {
            // Use NoSuchFileException rather than FileNotFoundException to stay consistent with LocalPhysicalKey.
            throw new NoSuchFileException(toUri().toString());
        } catch (S3Exception e) {
            throw new IOException("Could not read uri: " + toUri(), e);
        }
    }

    @Override
    public OpenResponse open() throws IOException {
        var inputStream = getObject();
        String effectiveVersionId = inputStream.response().versionId();
        return new OpenResponse(inputStream, new S3PhysicalKey(bucket, key, effectiveVersionId));
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return getObject();
    }

    @Override
    public void putBytes(byte[] bytes) throws IOException {
        S3Client s3 = getClient();

        PutObjectRequest objectRequest = PutObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .build();

        s3.putObject(objectRequest, RequestBody.fromBytes(bytes));
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

    @Override
    public Stream<String> listRecursively() throws IOException {
        S3Client s3 = getClient();

        String prefix = looksLikeDir() ? key : key + "/";

        ListObjectsV2Request listRequest = ListObjectsV2Request
            .builder()
            .bucket(bucket)
            .prefix(prefix)
            .build();

        ListObjectsV2Iterable listResp = s3.listObjectsV2Paginator(listRequest);
        return listResp
            .stream()
            .flatMap(r -> r.contents().stream())
            .map(obj -> obj.key().substring(prefix.length()));
    }
}
