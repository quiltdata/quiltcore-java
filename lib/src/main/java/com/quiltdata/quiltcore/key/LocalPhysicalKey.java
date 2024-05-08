package com.quiltdata.quiltcore.key;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Represents a local physical key that is used to access files on the local file system.
 * This class extends the abstract class PhysicalKey.
 */
public class LocalPhysicalKey extends PhysicalKey {

    private static int MAX_DEPTH = 1000;

    private final String path;

    /**
     * Constructs a LocalPhysicalKey with the specified path.
     *
     * @param path The absolute path of the file.
     * @throws IllegalArgumentException if the path is not absolute.
     */
    public LocalPhysicalKey(String path) {
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("Relative paths are not allowed");
        }

        this.path = path;
    }

    /**
     * Constructs a LocalPhysicalKey with the specified path.
     *
     * @param path The absolute path of the file.
     * @throws IllegalArgumentException if the path is not absolute.
     */
    public LocalPhysicalKey(Path path) {
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Relative paths are not allowed");
        }

        this.path = path.toString();
    }

    /**
     * Constructs a LocalPhysicalKey with the specified URI.
     *
     * @param uri The URI representing the file.
     * @throws IllegalArgumentException if the URI scheme, hostname, port, path, or query is unexpected.
     */
    public LocalPhysicalKey(URI uri) {
        if (uri.getScheme() == null || !uri.getScheme().equals("file")) {
            throw new IllegalArgumentException("Unexpected URI scheme: " + uri.getScheme());
        }
        if (uri.getHost() != null && !uri.getHost().equals("localhost")) {
            throw new IllegalArgumentException("Unexpected hostname");
        }
        if (uri.getPort() != -1) {
            throw new IllegalArgumentException("Unexpected port");
        }
        if (uri.getPath() == null || !uri.getPath().startsWith("/")) {
            throw new IllegalArgumentException("Unexpected path: " + uri.getPath());
        }
        if (uri.getQuery() != null) {
            throw new IllegalArgumentException("Unexpected query");
        }

        path = uri.getPath();
    }

    /**
     * Returns the path of the file.
     *
     * @return The path of the file.
     */
    public String getPath() {
        return path;
    }

    /**
     * Opens the file for reading.
     *
     * @return An OpenResponse object representing the opened file.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public OpenResponse open() throws IOException {
        return new OpenResponse(getInputStream(), this);
    }

    /**
     * Returns an InputStream for reading the file.
     *
     * @return An InputStream for reading the file.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public InputStream getInputStream() throws IOException {
        Path p = Path.of(path);
        return Files.newInputStream(p);
    }

    /**
     * Writes the specified bytes to the file.
     *
     * @param bytes The bytes to write.
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void putBytes(byte[] bytes) throws IOException {
        Path p = Path.of(path);
        Files.write(p, bytes);
    }

    /**
     * Resolves the specified child path against the current path.
     *
     * @param child The child path to resolve.
     * @return A new LocalPhysicalKey representing the resolved path.
     */
    @Override
    public PhysicalKey resolve(String child) {
        return new LocalPhysicalKey(joinPaths(path, child));
    }

    /**
     * Returns the URI representation of the file.
     *
     * @return The URI representation of the file.
     */
    @Override
    public URI toUri() {
        try {
            // NOTE: Need "" as authority rather than null to produce file:/// instead of file:/
            return new URI("file", "", path, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Should not happen!", e);
        }
    }

    /**
     * Returns a stream of file paths, recursively traversing the directory.
     *
     * @return A stream of file paths.
     * @throws IOException if an I/O error occurs.
     */
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
