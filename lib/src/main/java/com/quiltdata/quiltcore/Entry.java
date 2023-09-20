package com.quiltdata.quiltcore;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.quiltdata.quiltcore.key.PhysicalKey;

import software.amazon.awssdk.utils.BinaryUtils;


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
    private ObjectNode metadata;

    public Entry(PhysicalKey physicalKey, long size, Hash hash, ObjectNode metadata) {
        this.physicalKey = physicalKey;
        this.size = size;
        this.hash = hash;
        this.metadata = metadata == null ? JsonNodeFactory.instance.objectNode() : metadata.deepCopy();
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

    // All metadata, not just "user_meta"
    public ObjectNode getMetadata() {
        return metadata.deepCopy();
    }

    public byte[] getBytes() throws IOException {
        // TODO: Verify the hash.

        return physicalKey.getBytes();
    }

    public Entry withHash() throws IOException {
        if (hash != null) {
            return this;
        }

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // This should never happen, and if it does happen, there's nothing we can do.
            // Let's not require the caller to handle it.
            throw new RuntimeException(e);
        }

        try (InputStream in = physicalKey.getInputStream()) {
            byte[] buffer = new byte[4096];
            int count;
            while ((count = in.read(buffer)) != -1) {
                digest.update(buffer, 0, count);
            }
        }
        String hash = BinaryUtils.toHex(digest.digest());
        return new Entry(physicalKey, size, new Hash(HashType.SHA256, hash), metadata);
    }
}
