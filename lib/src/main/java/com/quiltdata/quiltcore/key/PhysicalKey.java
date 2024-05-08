package com.quiltdata.quiltcore.key;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.stream.Stream;

/**
 * Represents a physical key that can be used to access data.
 */
public abstract class PhysicalKey {

    /**
     * Represents the response when opening a physical key.
     */
    public static class OpenResponse {
        /**
         * The input stream associated with the opened physical key.
         */
        public final InputStream inputStream;

        /**
         * The effective physical key after resolving any redirects or aliases.
         */
        public final PhysicalKey effectivePhysicalKey;

        /**
         * Constructs an OpenResponse object.
         *
         * @param inputStream           The input stream associated with the opened physical key.
         * @param effectivePhysicalKey  The effective physical key after resolving any redirects or aliases.
         */
        public OpenResponse(InputStream inputStream, PhysicalKey effectivePhysicalKey) {
            this.inputStream = inputStream;
            this.effectivePhysicalKey = effectivePhysicalKey;
        }
    }

    /**
     * Opens the physical key and returns the OpenResponse object.
     *
     * @return The OpenResponse object containing the input stream and effective physical key.
     * @throws IOException If an I/O error occurs while opening the physical key.
     */
    public abstract OpenResponse open() throws IOException;

    /**
     * Returns an input stream for the physical key.
     *
     * @return The input stream for the physical key.
     * @throws IOException If an I/O error occurs while getting the input stream.
     */
    public abstract InputStream getInputStream() throws IOException;

    /**
     * Reads all bytes from the input stream and returns them as a byte array.
     *
     * @return The byte array containing all the bytes from the input stream.
     * @throws IOException If an I/O error occurs while reading the input stream.
     */
    public byte[] getBytes() throws IOException {
        try (InputStream in = getInputStream()) {
            return in.readAllBytes();
        }
    }

    /**
     * Writes the given byte array to the physical key.
     *
     * @param bytes The byte array to be written to the physical key.
     * @throws IOException If an I/O error occurs while writing to the physical key.
     */
    public abstract void putBytes(byte[] bytes) throws IOException;

    /**
     * Resolves the given child path relative to the current physical key.
     *
     * @param child The child path to be resolved.
     * @return The resolved physical key.
     */
    public abstract PhysicalKey resolve(String child);

    /**
     * Converts the physical key to a URI.
     *
     * @return The URI representation of the physical key.
     */
    public abstract URI toUri();

    /**
     * Returns a string representation of the physical key.
     *
     * @return The string representation of the physical key.
     */
    @Override
    public String toString() {
        return toUri().toString();
    }

    /**
     * Lists all files and directories recursively under the physical key.
     *
     * @return A stream of strings representing the files and directories.
     * @throws IOException If an I/O error occurs while listing the files and directories.
     */
    public abstract Stream<String> listRecursively() throws IOException;

    /**
     * Creates a PhysicalKey object from the given URI.
     *
     * @param uri The URI representing the physical key.
     * @return The PhysicalKey object corresponding to the URI.
     * @throws IllegalArgumentException If the URI scheme is not supported.
     */
    public static PhysicalKey fromUri(URI uri) {
        String scheme = uri.getScheme();
        if (scheme.equals("file")) {
            return new LocalPhysicalKey(uri);
        } else if (scheme.equals("s3")) {
            return new S3PhysicalKey(uri);
        } else {
            throw new IllegalArgumentException("Unsupported URI scheme: " + scheme);
        }
    }

    /**
     * Joins the parent path and child path to create a new path.
     *
     * @param parent The parent path.
     * @param child  The child path.
     * @return The joined path.
     */
    protected static String joinPaths(String parent, String child) {
        StringBuilder sb = new StringBuilder(parent.length() + child.length() + 1);
        if (parent.length() > 0) {
            sb.append(parent);
            if (parent.charAt(parent.length() - 1) != '/') {
                sb.append('/');
            }
        }

        sb.append(child.charAt(0) == '/' ? child.substring(1) : child);
        return sb.toString();
    }
}
