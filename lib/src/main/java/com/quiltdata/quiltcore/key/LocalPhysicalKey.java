package com.quiltdata.quiltcore.key;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class LocalPhysicalKey extends PhysicalKey {
    private static int MAX_DEPTH = 1000;

    private String path;

    public LocalPhysicalKey(String path) {
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("Relative paths are not allowed");
        }

        this.path = path;
    }

    public LocalPhysicalKey(Path path) {
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Relative paths are not allowed");
        }

        this.path = path.toString();
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
    public InputStream getInputStream() throws IOException {
        Path p = Path.of(path);
        return Files.newInputStream(p);
    }

    @Override
    public void putBytes(byte[] bytes) throws IOException {
        Path p = Path.of(path);
        Files.write(p, bytes);
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

    @Override
    public Stream<String> listRecursively() throws IOException {
        Path pathObj = Path.of(path);

        return Files.find(
            pathObj,
            MAX_DEPTH,
            (child, attrs) -> attrs.isRegularFile()
        ).map(child -> pathObj.relativize(child).toString());
    }
}
