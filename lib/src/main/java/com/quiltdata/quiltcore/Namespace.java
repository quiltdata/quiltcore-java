package com.quiltdata.quiltcore;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import com.quiltdata.quiltcore.key.PhysicalKey;


public class Namespace {
    private PhysicalKey path;
    private PhysicalKey versions;

    public Namespace(PhysicalKey path, PhysicalKey versions) {
        this.path = path;
        this.versions = versions;
    }

    public String getHash(String tag) throws IOException {
        return new String(path.resolve(tag).getBytes(), StandardCharsets.UTF_8);
    }

    public Manifest getManifest(String hash) throws IOException, URISyntaxException {
        return new Manifest(versions.resolve(hash));
    }
}
