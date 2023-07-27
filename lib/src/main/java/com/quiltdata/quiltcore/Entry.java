package com.quiltdata.quiltcore;

import java.io.IOException;

import com.quiltdata.quiltcore.key.PhysicalKey;


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

        return physicalKey.getBytes();
    }
}
