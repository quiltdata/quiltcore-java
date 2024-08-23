package com.quiltdata.quiltcore;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.util.Map;

public class ManifestTest {

    @Test
    void testParseQuiltURI() {
        // Arrange
        String quiltURI = "quilt+s3://bkt#package=prefix/suffix@top_hash&path=file";
        try {
            URI uri = new URI(quiltURI);
            Map<String, String> result = Manifest.ParseQuiltURI(uri);
            assertEquals("bkt", result.get("bucket"));
            assertEquals("prefix/suffix", result.get("package"));
            assertEquals("top_hash", result.get("hash"));
            assertEquals("latest", result.get("revision"));
            assertEquals("file", result.get("path"));
        } catch (Exception e) {
            fail("Failed to create URI from quiltURI", e);
        }
    }

    @Test
    void testFromQuiltURI() {
        // Arrange
        String quiltURI = "quilt+s3://quilt-example#package=examples/metadata";
        try {
            Manifest manifest = Manifest.FromQuiltURI(quiltURI);
            assert manifest != null;
        } catch (Exception e) {
            fail("Failed to create URI from quiltURI", e);
        }
    }

    @Test
    void testFromChunkedURI() {
        // Arrange
        String quiltURI = "quilt+s3://udp-spec#package=nf-quilt/source";
        try {
            Manifest manifest = Manifest.FromQuiltURI(quiltURI);
            assert manifest != null;
        } catch (Exception e) {
            fail("Failed to create URI from quiltURI", e);
        }
    }

}
