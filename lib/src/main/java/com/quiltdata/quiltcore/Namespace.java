package com.quiltdata.quiltcore;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import com.quiltdata.quiltcore.key.PhysicalKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a namespace in the Quilt Core library.
 */
public class Namespace {
    private static final Logger logger = LoggerFactory.getLogger(Namespace.class);

    private final Registry registry;
    private final String name;
    private final PhysicalKey path;
    private final PhysicalKey versions;

    /**
     * Constructs a new Namespace object.
     *
     * @param registry The registry associated with the namespace.
     * @param name The name of the namespace.
     * @param path The physical key representing the path of the namespace.
     * @param versions The physical key representing the versions of the namespace.
     */
    public Namespace(Registry registry, String name, PhysicalKey path, PhysicalKey versions) {
        this.registry = registry;
        this.name = name;
        this.path = path;
        this.versions = versions;
    }

    /**
     * Returns the registry associated with the namespace.
     *
     * @return The registry.
     */
    public Registry getRegistry() {
        return registry;
    }

    /**
     * Returns the name of the namespace.
     *
     * @return The name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the physical key representing the path of the namespace.
     *
     * @return The path.
     */
    public PhysicalKey getPath() {
        return path;
    }

    /**
     * Returns the physical key representing the versions of the namespace.
     *
     * @return The versions.
     */
    public PhysicalKey getVersions() {
        return versions;
    }

    /**
     * Returns the hash associated with the given tag.
     *
     * @param tag The tag.
     * @return The hash.
     * @throws IOException If an I/O error occurs.
     */
    public String getHash(String tag) throws IOException {
        logger.debug("Resolving tag: {}", tag);
        return new String(path.resolve(tag).getBytes(), StandardCharsets.UTF_8);
    }

    /**
     * Resolves the hash prefix to a complete hash.
     *
     * @param hashPrefix The hash prefix.
     * @return The resolved hash.
     * @throws IOException If an I/O error occurs.
     */
    public String resolveHash(String hashPrefix) throws IOException {
        int len = hashPrefix.length();
        if (len == 64) {
            return hashPrefix;
        } else if (len >= 6 && len < 64) {
            String[] matching = versions
                .listRecursively()
                .filter(hash -> hash.startsWith(hashPrefix))
                .toArray(String[]::new);
            if (matching.length == 0) {
                throw new IOException("Found zero matches for " + hashPrefix);
            } else if (matching.length > 1) {
                throw new IOException("Found multiple matches for " + hashPrefix);
            } else {
                return matching[0];
            }
        } else {
            throw new IllegalArgumentException("Invalid hash prefix: " + hashPrefix);
        }
    }

    /**
     * Returns the manifest associated with the given hash.
     *
     * @param hash The hash.
     * @return The manifest.
     * @throws IOException If an I/O error occurs.
     * @throws URISyntaxException If the URI syntax is invalid.
     */
    public Manifest getManifest(String hash) throws IOException, URISyntaxException {
        logger.debug("Resolving hash: {}", hash);
        String resolvedHash = resolveHash(hash);
        return Manifest.createFromFile(versions.resolve(resolvedHash));
    }
}
