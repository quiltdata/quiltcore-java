package com.quiltdata.quiltcore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

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

    @Test
    public void testLocalList() {
        try {
            Path dir = Path.of("src", "test", "resources", "dir");
            LocalPhysicalKey pk = new LocalPhysicalKey(dir.toAbsolutePath().toString());
            String[] files = pk.listRecursively().toArray(String[]::new);
            Arrays.sort(files);
            assertArrayEquals(new String[] { "a/blah", "bar.txt", "foo.txt" }, files);
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testS3List() {
        try {
            S3PhysicalKey pk = new S3PhysicalKey("quilt-example", "examples/hurdat/", null);
            String[] files = pk.listRecursively().toArray(String[]::new);
            Arrays.sort(files);
            assertArrayEquals(new String[] {
                "data/atlantic-storms.csv",
                "notebooks/QuickStart.ipynb",
                "quilt_summarize.json",
                "scripts/build.py",
            }, files);
        } catch (IOException e) {
            fail();
        }
    }
}
