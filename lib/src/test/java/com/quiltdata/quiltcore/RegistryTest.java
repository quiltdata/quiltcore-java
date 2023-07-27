package com.quiltdata.quiltcore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.quiltdata.quiltcore.key.PhysicalKey;


public class RegistryTest {
    @Rule
    public TemporaryFolder installFolder = new TemporaryFolder();

    @Test
    public void testS3() {
        try {
            PhysicalKey p = PhysicalKey.fromUri(new URI("s3://quilt-example/"));

            Registry r = new Registry(p);
            Namespace n = r.getNamespace("examples/metadata");
            String hash = n.getHash("latest");
            Manifest m = n.getManifest(hash);

            Entry e = m.getEntry("README.md");

            byte[] data = e.getBytes();
            String readme = new String(data);

            assertTrue(readme.startsWith("# Working with metadata in Quilt packages"));

            Path dest = installFolder.newFolder().toPath();
            m.install(dest);

            assertTrue(Files.exists(dest.resolve("README.md")));
            assertTrue(Files.exists(dest.resolve("quilt_summarize.json")));
            assertTrue(Files.exists(dest.resolve("cluster0v2.csv")));
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } catch (URISyntaxException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testResolveHash() {
        try {
            PhysicalKey p = PhysicalKey.fromUri(new URI("s3://quilt-example/"));

            Registry r = new Registry(p);
            Namespace n = r.getNamespace("examples/metadata");
            String latest = n.getHash("latest");

            assertEquals(latest, n.resolveHash("fd2044"));
            assertThrows(IOException.class, () -> n.resolveHash("ffffff"));
            assertThrows(IllegalArgumentException.class, () -> n.resolveHash("abc"));
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } catch (URISyntaxException e) {
            e.printStackTrace();
            fail();
        }
    }
}
