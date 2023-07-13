package com.quiltdata.quiltcore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


public class Namespace {
    private Path path;
    private Path versions;

    public Namespace(Path path, Path versions) {
        this.path = path;
        this.versions = versions;
    }

    public String getHash(String tag) throws IOException {
        return Files.readString(path.resolve(tag));
    }

    public Manifest getManifest(String tag) throws IOException {
        String hash = getHash(tag);
        return new Manifest(versions.resolve(hash));
    }
}
