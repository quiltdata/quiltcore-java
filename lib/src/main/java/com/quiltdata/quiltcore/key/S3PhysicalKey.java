package com.quiltdata.quiltcore.key;

public class S3PhysicalKey extends PhysicalKey {
    private String bucket;
    private String key;
    private String versionId;

    public S3PhysicalKey(String bucket, String key, String versionId) {
        this.bucket = bucket;
        this.key = key;
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
}
