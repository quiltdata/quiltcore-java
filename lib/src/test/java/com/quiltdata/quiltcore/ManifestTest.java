package com.quiltdata.quiltcore;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.util.Map;

public class ManifestTest {

    @Test
    void testParseQuiltURI() {
        // Arrange
        String quiltURI = "quilt+s3://bucket#package=package@hash&path=path";
        try {
            URI uri = new URI(quiltURI);
            Map<String, String> result = Manifest.parseQuiltURI(uri);
            assertEquals("bucket", result.get("bucket"));
            assertEquals("package", result.get("package"));
            assertEquals("hash", result.get("hash"));
            assertEquals("path", result.get("path"));
        } catch (Exception e) {
            fail("Failed to create URI from quiltURI");
        }
    }
}
