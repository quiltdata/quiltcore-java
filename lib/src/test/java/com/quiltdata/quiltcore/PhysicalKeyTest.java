package com.quiltdata.quiltcore;

import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import software.amazon.awssdk.services.s3.model.S3Exception;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.S3PhysicalKey;



public class PhysicalKeyTest {
    @Test
    public void testFileUris() throws Exception {
        String uri = "file:///foo/a+b%20c";
        LocalPhysicalKey pk = new LocalPhysicalKey(new URI(uri));

        assertEquals("/foo/a+b c", pk.getPath());
        assertEquals(uri, pk.toUri().toString());
    }

    @Test
    public void testS3Uris() throws Exception {
        String uri = "s3://bucket/foo/a+b%20c?versionId=a_b";
        S3PhysicalKey pk = new S3PhysicalKey(new URI(uri));

        assertEquals("bucket", pk.getBucket());
        assertEquals("foo/a+b c", pk.getKey());
        assertEquals("a_b", pk.getVersionId());

        assertEquals(uri, pk.toUri().toString());  // Breaks for %2f in VersionID
    }

    @Test
    @DisabledOnOs({ OS.WINDOWS })
    public void testLocalList() throws Exception {
        Path dir = Path.of("src", "test", "resources", "dir");
        LocalPhysicalKey pk = new LocalPhysicalKey(dir.toAbsolutePath().toString());
        String[] files = pk.listRecursively().toArray(String[]::new);
        Arrays.sort(files);
        assertArrayEquals(new String[] { "a/blah", "bar.txt", "foo.txt" }, files);
    }

    @Test
    public void testS3List() throws Exception {
        S3PhysicalKey pk = new S3PhysicalKey("quilt-example", "examples/hurdat/", null);
        String[] files = pk.listRecursively().toArray(String[]::new);
        Arrays.sort(files);
        assertArrayEquals(new String[] {
            "data/atlantic-storms.csv",
            "notebooks/QuickStart.ipynb",
            "quilt_summarize.json",
            "scripts/build.py",
        }, files);
    }

    @Test
    public void testS3GetReadOnly() throws Exception {
        S3PhysicalKey pk_in = new S3PhysicalKey("allencell", "README.md", null);
        S3PhysicalKey pk_out = new S3PhysicalKey("allencell", "test/tmp/README.md", null);
        byte[] bytes = pk_in.getBytes();
        assertTrue(bytes.length > 0);
        assertThrows(S3Exception.class, () -> pk_out.putBytes(bytes));
    }
}
