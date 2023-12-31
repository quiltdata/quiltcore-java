package com.quiltdata.quiltcore;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import com.quiltdata.quiltcore.key.PhysicalKey;


public class Namespace {
    private final Registry registry;
    private final String name;
    private final PhysicalKey path;
    private final PhysicalKey versions;

    public Namespace(Registry registry, String name, PhysicalKey path, PhysicalKey versions) {
        this.registry = registry;
        this.name = name;
        this.path = path;
        this.versions = versions;
    }

    public Registry getRegistry() {
        return registry;
    }

    public String getName() {
        return name;
    }

    public PhysicalKey getPath() {
        return path;
    }

    public PhysicalKey getVersions() {
        return versions;
    }

    public String getHash(String tag) throws IOException {
        return new String(path.resolve(tag).getBytes(), StandardCharsets.UTF_8);
    }

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

    public Manifest getManifest(String hash) throws IOException, URISyntaxException {
        String resolvedHash = resolveHash(hash);
        return Manifest.createFromFile(versions.resolve(resolvedHash));
    }
}
