package com.quiltdata.quiltcore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.PhysicalKey;


public class RegistryTest {
    @Rule
    public TemporaryFolder installFolder = new TemporaryFolder();

    @Test
    public void testLocalPackage() {
        try {
            Path dir = Path.of("src", "test", "resources", "packages").toAbsolutePath();
            PhysicalKey p = new LocalPhysicalKey(dir);

            Registry r = new Registry(p);
            Namespace n = r.getNamespace("test/test");
            String hash = n.getHash("latest");
            Manifest m = n.getManifest(hash);

            assertEquals(hash, m.calculateTopHash());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } catch (URISyntaxException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testS3Install() {
        try {
            PhysicalKey p = PhysicalKey.fromUri(new URI("s3://quilt-example/"));

            Registry r = new Registry(p);
            Namespace n = r.getNamespace("examples/metadata");
            String hash = n.getHash("latest");
            Manifest m = n.getManifest(hash);

            ObjectNode metadata = m.getMetadata();
            assertEquals("use realworld data", metadata.get("message").asText());
            assertEquals(4782, metadata.get("user_meta").get("dna_surface_area").asInt());

            Entry e = m.getEntry("README.md");

            byte[] data = e.getBytes();
            String readme = new String(data);

            assertTrue(readme.startsWith("# Working with metadata in Quilt packages"));

            assertTrue(e.getMetadata().isEmpty());

            assertEquals(hash, m.calculateTopHash());

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

    @Test
    public void testS3Push() {
        try {
            Path dir = Path.of("src", "test", "resources", "dir").toAbsolutePath();

            PhysicalKey p = PhysicalKey.fromUri(new URI("s3://quilt-dima2/"));

            Registry r = new Registry(p);
            Namespace n = r.getNamespace("dima/java_test");

            Path foo = dir.resolve("foo.txt");
            Path bar = dir.resolve("bar.txt");

            Manifest.Builder b = Manifest.builder();
            b.addEntry("foo", new Entry(new LocalPhysicalKey(foo), Files.size(bar), null, null));
            b.addEntry("bar", new Entry(new LocalPhysicalKey(bar), Files.size(bar), null, null));
            Manifest m = b.build();

            Manifest m2 = m.push(n, null);

            String topHash = m2.calculateTopHash();

            assertEquals("b9033f634856bd48cf9eb12d8e9cd75e2d4d6975e63544b009b8ec1dd349f861", topHash);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } catch (URISyntaxException e) {
            e.printStackTrace();
            fail();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            fail();
        }
    }
}
