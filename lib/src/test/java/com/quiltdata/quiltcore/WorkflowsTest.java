package com.quiltdata.quiltcore;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.PhysicalKey;
import com.quiltdata.quiltcore.workflows.ConfigDataVersion;
import com.quiltdata.quiltcore.workflows.WorkflowConfig;
import com.quiltdata.quiltcore.workflows.WorkflowException;
import com.quiltdata.quiltcore.workflows.WorkflowValidator;


public class WorkflowsTest {
    @Test
    public void testConfigDataVersion() {
        assertEquals(ConfigDataVersion.parse("1.2.3"), new ConfigDataVersion(1, 2, 3));
        assertEquals(ConfigDataVersion.parse("1.2"),   new ConfigDataVersion(1, 2, 0));
        assertEquals(ConfigDataVersion.parse("1"),     new ConfigDataVersion(1, 0, 0));

        assertTrue(ConfigDataVersion.parse("1.2.3").compareTo(ConfigDataVersion.parse("1.2.3")) == 0);
        assertTrue(ConfigDataVersion.parse("1.2.3").compareTo(ConfigDataVersion.parse("1.2.2")) > 0);
        assertTrue(ConfigDataVersion.parse("1.2.3").compareTo(ConfigDataVersion.parse("1.3")) < 0);
        assertTrue(ConfigDataVersion.parse("1.2").compareTo(ConfigDataVersion.parse("2")) < 0);
    }

    @Test
    @IgnoreIf({ System.getProperty("os.name").contains("indows") })
    public void testWorkflows() throws Exception {
        Path path = Path.of("src", "test", "resources", "config.yml").toAbsolutePath();
        PhysicalKey key = new LocalPhysicalKey(path);
        WorkflowConfig config = WorkflowConfig.load(key);

        assertNotEquals(null, config);

        WorkflowValidator validator = config.getWorkflowValidator("alpha");

        JsonNode data = validator.getDataToStore();
        assertEquals("alpha", data.get("id").asText());
        assertEquals(key.toString(), data.get("config").asText());
        assertTrue(data.get("schemas").get("meta").asText().contains("?versionId="));

        ObjectNode pkgMeta = JsonNodeFactory.instance.objectNode()
            .put("VERSION", "v0")
            .put("user_meta", "blah");
        ObjectNode readmeMeta = JsonNodeFactory.instance.objectNode()
            .put("foo", "bar");
        Entry readme = new Entry(null, 10, null, readmeMeta);

        // Good package.
        validator.validate("test/foo", Map.of("README.md", readme), pkgMeta, "blah");

        // Bad name.
        assertThrows(WorkflowException.class, () -> {
            validator.validate("zzz/foo", Map.of("README.md", readme), pkgMeta, "blah");
        });

        // Missing commit message.
        assertThrows(WorkflowException.class, () -> {
            validator.validate("test/foo", Map.of("README.md", readme), pkgMeta, null);
        });

        // Missing README.
        assertThrows(WorkflowException.class, () -> {
            validator.validate("test/foo", Map.of(), pkgMeta, "blah");
        });

        // Wrong package metadata.
        assertThrows(WorkflowException.class, () -> {
            validator.validate("test/foo", Map.of("README.md", readme), readmeMeta, "blah");
        });
    }
}
