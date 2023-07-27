package com.quiltdata.quiltcore;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

import org.junit.Test;

import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.S3PhysicalKey;


public class PhysicalKeyTest {
    @Test
    public void testFileUris() {
        try {
            String uri = "file:///foo/a+b%20c";
            LocalPhysicalKey pk = new LocalPhysicalKey(new URI(uri));

            assertEquals("/foo/a+b c", pk.getPath());
            assertEquals(uri, pk.toUri().toString());
        } catch (URISyntaxException e) {
            fail();
        }
    }

    @Test
    public void testS3Uris() {
        try {
            String uri = "s3://bucket/foo/a+b%20c?versionId=a%2Fb";
            S3PhysicalKey pk = new S3PhysicalKey(new URI(uri));

            assertEquals("bucket", pk.getBucket());
            assertEquals("foo/a+b c", pk.getKey());
            assertEquals("a/b", pk.getVersionId());
            assertEquals(uri, pk.toUri().toString());  // BROKEN!!!
        } catch (URISyntaxException e) {
            fail();
        }
    }
}
