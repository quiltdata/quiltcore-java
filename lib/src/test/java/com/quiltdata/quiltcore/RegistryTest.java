package com.quiltdata.quiltcore;

import java.io.IOException;
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


public class RegistryTest {
    String READ_BUCKET = "s3://quilt-example";
    String WRITE_BUCKET = "s3://quilt-dima2";

    @Test
    @DisabledOnOs({ OS.WINDOWS })
    public void testLocalPackage() throws Exception {
        Path dir = Path.of("src", "test", "resources", "packages");
        String dirURI = dir.toUri().toString();
        Namespace n = Registry.createNamespaceAtUri("test/test", dirURI);
        String hash = n.getHash("latest");
        Manifest m = n.getManifest(hash);

        assertEquals(hash, m.calculateTopHash());
    }

    @Test
    public void testS3Install(@TempDir Path dest) throws Exception {
        Namespace n = Registry.createNamespaceAtUri("examples/metadata", READ_BUCKET);
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
        Namespace n = Registry.createNamespaceAtUri("examples/metadata", READ_BUCKET);
        String latest = n.getHash("latest");

        assertEquals(latest, n.resolveHash("fd2044"));
        assertThrows(IOException.class, () -> n.resolveHash("ffffff"));
        assertThrows(IllegalArgumentException.class, () -> n.resolveHash("abc"));
    }

    @Test
    public void testS3Push() throws Exception {
        Namespace n = Registry.createNamespaceAtUri("dima/java_test", WRITE_BUCKET);
        Path dir = Path.of("src", "test", "resources", "dir").toAbsolutePath();

        Path foo = dir.resolve("foo.txt");
        Path bar = dir.resolve("bar.txt");

        Manifest.Builder b = Manifest.builder();
        b.addEntry("foo", new Entry(new LocalPhysicalKey(foo), Files.size(foo), null, null));
        b.addEntry("bar", new Entry(new LocalPhysicalKey(bar), Files.size(bar), null, null));
        Manifest m = b.build();

        Manifest m2 = m.push(n, null, null);

        assertTrue(m2.getMetadata().get("workflow").get("id").isNull());

        String topHash = m2.calculateTopHash();

        assertEquals("e329b376d98083054be18881f3471fff7957acdfca8b02c54946f082dc56b572", topHash);
    }

    @Test
    public void testS3PushErrors() throws Exception {
        Path dir = Path.of("src", "test", "resources", "dir").toAbsolutePath();
        Namespace n = Registry.createNamespaceAtUri("dima/java_test", WRITE_BUCKET);

        Path bad = dir.resolve("no such file.txt");

        Manifest.Builder b = Manifest.builder();
        b.addEntry("foo", new Entry(new LocalPhysicalKey(bad), 123, null, null));
        Manifest m = b.build();

        assertThrows(IOException.class, () -> m.push(n, null, ""));
    }

    @Test
    public void testS3WorkflowPush() throws Exception {
        Namespace n = Registry.createNamespaceAtUri("dima/java_workflow_test", WRITE_BUCKET);

        Path dir = Path.of("src", "test", "resources", "dir").toAbsolutePath();
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
