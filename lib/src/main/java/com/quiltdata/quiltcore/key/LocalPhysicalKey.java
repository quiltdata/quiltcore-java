package com.quiltdata.quiltcore.key;

public class LocalPhysicalKey extends PhysicalKey {
    private String path;

    public LocalPhysicalKey(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
