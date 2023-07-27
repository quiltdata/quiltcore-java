package com.quiltdata.quiltcore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.PhysicalKey;
import com.quiltdata.quiltcore.key.S3PhysicalKey;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;


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

    private PhysicalKey physicalKey;
    private long size;
    private Hash hash;

    public Entry(PhysicalKey physicalKey, long size, Hash hash) {
        this.physicalKey = physicalKey;
        this.size = size;
        this.hash = hash;
    }

    public PhysicalKey getPhysicalKey() {
        return physicalKey;
    }

    public long getSize() {
        return size;
    }

    public Hash getHash() {
        return hash;
    }

    public byte[] getBytes() throws IOException {
        // TODO: Verify the hash.

        if (physicalKey instanceof LocalPhysicalKey) {
            LocalPhysicalKey localKey = (LocalPhysicalKey)physicalKey;
            Path p = Path.of(localKey.getPath());
            return Files.readAllBytes(p);
        } else if (physicalKey instanceof S3PhysicalKey) {
            S3PhysicalKey s3Key = (S3PhysicalKey)physicalKey;
            String bucket = s3Key.getBucket();
            String key = s3Key.getKey();
            String versionId = s3Key.getVersionId();

            S3AsyncClient s3 = S3ClientStore.getClient(bucket);

            GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .bucket(bucket)
                .key(key)
                .versionId(versionId)
                .build();

            try {
                ResponseBytes<GetObjectResponse> objectBytes =
                    s3.getObject(objectRequest, AsyncResponseTransformer.toBytes()).get();
                return objectBytes.asByteArray();
            } catch (S3Exception e) {
                // Convert into IOException for consistency with file://
                throw new IOException("Could not read uri: " + physicalKey, e);
            } catch (InterruptedException e) {
                throw new IOException("Could not read uri: " + physicalKey, e);
            } catch (ExecutionException e) {
                throw new IOException("Could not read uri: " + physicalKey, e);
            }
        } else {
            throw new IOException("Unsupported key: " + physicalKey);
        }
    }
}
