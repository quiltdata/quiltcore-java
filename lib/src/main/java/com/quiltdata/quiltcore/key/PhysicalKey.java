package com.quiltdata.quiltcore.key;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.stream.Stream;

public abstract class PhysicalKey {
    public abstract InputStream getInputStream() throws IOException;

    public byte[] getBytes() throws IOException {
        try (InputStream in = getInputStream()) {
            return in.readAllBytes();
        }
    }

    public abstract PhysicalKey resolve(String child);

    public abstract URI toUri();

    // TODO: Include file sizes?
    public abstract Stream<String> listRecursively() throws IOException;

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
