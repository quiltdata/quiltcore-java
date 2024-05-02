package com.quiltdata.quiltcore;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.PhysicalKey;


public class RegistryTest {
    @Test
    @DisabledOnOs({ OS.WINDOWS })
    public void testLocalPackage() throws Exception {
        Path dir = Path.of("src", "test", "resources", "packages").toAbsolutePath();
        PhysicalKey p = new LocalPhysicalKey(dir);

        Registry r = new Registry(p);
        Namespace n = r.getNamespace("test/test");
        String hash = n.getHash("latest");
        Manifest m = n.getManifest(hash);

        assertEquals(hash, m.calculateTopHash());
    }

    @Test
    public void testS3Install(@TempDir Path dest) throws Exception {
        PhysicalKey p = PhysicalKey.fromUri(new URI("s3://quilt-example/"));

        Registry r = new Registry(p);
        Namespace n = r.getNamespace("examples/metadata");
        String hash = n.getHash("latest");
        Manifest m = n.getManifest(hash);

        ObjectNode metadata = m.getMetadata();
        assertEquals("use realworld data", metadata.get("message").asText());
        assertEquals(4782, metadata.get("user_meta").get("dna_surface_area").asInt());

        Entry e = m.getEntries().get("README.md");

        byte[] data = e.getBytes();
        String readme = new String(data);

        assertTrue(readme.startsWith("# Working with metadata in Quilt packages"));

        assertTrue(e.getMetadata().isEmpty());

        assertEquals(hash, m.calculateTopHash());

        m.install(dest);

        assertTrue(Files.exists(dest.resolve("README.md")));
        assertTrue(Files.exists(dest.resolve("quilt_summarize.json")));
        assertTrue(Files.exists(dest.resolve("cluster0v2.csv")));
    }

    @Test
    public void testResolveHash() throws Exception {
        PhysicalKey p = PhysicalKey.fromUri(new URI("s3://quilt-example/"));

        Registry r = new Registry(p);
        Namespace n = r.getNamespace("examples/metadata");
        String latest = n.getHash("latest");

        assertEquals(latest, n.resolveHash("fd2044"));
        assertThrows(IOException.class, () -> n.resolveHash("ffffff"));
        assertThrows(IllegalArgumentException.class, () -> n.resolveHash("abc"));
    }

    @Test
    public void testS3Push() throws Exception {
        Path dir = Path.of("src", "test", "resources", "dir").toAbsolutePath();

        PhysicalKey p = PhysicalKey.fromUri(new URI("s3://quilt-dima2/"));

        Registry r = new Registry(p);
        Namespace n = r.getNamespace("dima/java_test");

        Path foo = dir.resolve("foo.txt");
        Path bar = dir.resolve("bar.txt");

        Manifest.Builder b = Manifest.builder();
        b.addEntry("foo", new Entry(new LocalPhysicalKey(foo), Files.size(foo), null, null));
        b.addEntry("bar", new Entry(new LocalPhysicalKey(bar), Files.size(bar), null, null));
        Manifest m = b.build();

        Manifest m2 = m.push(n, null, "");

        assertTrue(m2.getMetadata().get("workflow").get("id").isNull());

        String topHash = m2.calculateTopHash();

        assertEquals("97ea1254aab77d28e0f459a739351879b46e6e6142efdab327abf6ab6bdf72dd", topHash);
    }

    @Test
    public void testS3PushErrors() throws Exception {
        Path dir = Path.of("src", "test", "resources", "dir").toAbsolutePath();

        PhysicalKey p = PhysicalKey.fromUri(new URI("s3://quilt-dima2/"));

        Registry r = new Registry(p);
        Namespace n = r.getNamespace("dima/java_test");

        Path bad = dir.resolve("no such file.txt");

        Manifest.Builder b = Manifest.builder();
        b.addEntry("foo", new Entry(new LocalPhysicalKey(bad), 123, null, null));
        Manifest m = b.build();

        assertThrows(IOException.class, () -> m.push(n, null, ""));
    }

    @Test
    public void testS3WorkflowPush() throws Exception {
        Path dir = Path.of("src", "test", "resources", "dir").toAbsolutePath();

        PhysicalKey p = PhysicalKey.fromUri(new URI("s3://quilt-dima2/"));

        Registry r = new Registry(p);
        Namespace n = r.getNamespace("dima/java_workflow_test");

        Path foo = dir.resolve("foo.txt");

        Manifest.Builder b = Manifest.builder();
        ObjectNode packageMeta = JsonNodeFactory.instance.objectNode()
            .put("version", "v0")
            .put("user_meta", "123456");
        b.setMetadata(packageMeta);
        ObjectNode meta = JsonNodeFactory.instance.objectNode().put("foo", "bar");
        b.addEntry("README.md", new Entry(new LocalPhysicalKey(foo), Files.size(foo), null, meta));
        Manifest m = b.build();

        Manifest m2 = m.push(n, "commit stuff", "alpha");

        assertEquals("alpha", m2.getMetadata().get("workflow").get("id").asText());
    }
}
