package com.quiltdata.quiltcore.key;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalPhysicalKey extends PhysicalKey {
    private String path;

    public LocalPhysicalKey(String path) {
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("Relative paths are not allowed");
        }

        this.path = path;
    }

    public LocalPhysicalKey(URI uri) {
        if (!uri.getScheme().equals("file")) {
            throw new IllegalArgumentException("Unexpected URI scheme: " + uri.getScheme());
        }
        if (uri.getHost() != null && !uri.getHost().equals("localhost")) {
            throw new IllegalArgumentException("Unexpected hostname");
        }
        if (uri.getPort() != -1) {
            throw new IllegalArgumentException("Unexpected port");
        }
        if (uri.getQuery() != null) {
            throw new IllegalArgumentException("Unexpected query");
        }

        path = uri.getPath();
    }

    public String getPath() {
        return path;
    }

    @Override
    public byte[] getBytes() throws IOException {
        Path p = Path.of(path);
        return Files.readAllBytes(p);
    }

    @Override
    public PhysicalKey resolve(String child) {
        return new LocalPhysicalKey(joinPaths(path, child));
    }

    @Override
    public URI toUri() {
        try {
            // NOTE: Need "" as authority rather than null to produce file:/// instead of file:/
            return new URI("file", "", path, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Should not happen!", e);
        }
    }
}
