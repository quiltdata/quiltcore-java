package com.quiltdata.quiltcore;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


public class RegistryTest {
    String READ_BUCKET = "s3://quilt-example";
    String WRITE_BUCKET = "s3://quilt-dima2";

    @Test
    @DisabledOnOs({ OS.WINDOWS })
    public void testLocalPackage() throws Exception {
        Path dir = Path.of("src", "test", "resources", "packages");
        String dirURI = dir.toUri().toString();
        Namespace n = Registry.CreateNamespaceAtUri("test/test", dirURI);
        String hash = n.getHash("latest");
        Manifest m = n.getManifest(hash);

        assertEquals(hash, m.calculateTopHash());
    }

    @Test
    public void testS3Install(@TempDir Path dest) throws Exception {
        Namespace n = Registry.CreateNamespaceAtUri("examples/metadata", READ_BUCKET);
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
        Namespace n = Registry.CreateNamespaceAtUri("examples/metadata", READ_BUCKET);
        String latest = n.getHash("latest");

        assertEquals(latest, n.resolveHash("fd2044"));
        assertThrows(IOException.class, () -> n.resolveHash("ffffff"));
        assertThrows(IllegalArgumentException.class, () -> n.resolveHash("abc"));
    }

    @Test
    public void testS3Push() throws Exception {
        Namespace n = Registry.CreateNamespaceAtUri("dima/java_test", WRITE_BUCKET);
        Path dir = Path.of("src", "test", "resources", "dir").toAbsolutePath();

        Manifest m = Manifest.BuildFromDir(dir, null, ".*\\.txt");
        Manifest m2 = m.push(n, null, null);

        assertTrue(m2.getMetadata().get("workflow").get("id").isNull());

        String topHash = m2.calculateTopHash();

        assertEquals("71cba8513adfbe7dc4e4b4e03d9e71ba36c3f10745cc4f54949028c2339a35e2", topHash);
    }

    @Test
    public void testS3PushErrors() throws Exception {
        Path dir = Path.of("src", "test", "resources", "dir").toAbsolutePath();
        Namespace n = Registry.CreateNamespaceAtUri("dima/java_test", WRITE_BUCKET);

        Path bad = dir.resolve("no such file.txt");

        Manifest.Builder b = Manifest.builder();
        b.addEntry("foo", new Entry(new LocalPhysicalKey(bad), 123, null, null));
        Manifest m = b.build();

        assertThrows(IOException.class, () -> m.push(n, null, ""));
    }

    @Test
    public void testS3WorkflowPush() throws Exception {
        Namespace n = Registry.CreateNamespaceAtUri("dima/java_workflow_test", WRITE_BUCKET);

        Path dir = Path.of("src", "test", "resources", "dir").toAbsolutePath();
        Path foo = dir.resolve("foo.txt");
        Object user_meta = "123456";
        Map<String, Path> paths = Map.of("README.md", foo);

        ObjectNode meta = JsonNodeFactory.instance.objectNode().put("foo", "bar");
        Map<String, ObjectNode> object_meta = Map.of("README.md", meta);
        Manifest m = Manifest.BuildFromPaths(paths, user_meta, object_meta);

        Manifest m2 = m.push(n, "commit stuff", "alpha");

        assertEquals("alpha", m2.getMetadata().get("workflow").get("id").asText());
    }
}
