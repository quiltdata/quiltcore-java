package com.quiltdata.quiltcore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.S3PhysicalKey;
import com.quiltdata.quiltcore.workflows.ConfigDataVersion;
import com.quiltdata.quiltcore.workflows.ConfigurationException;
import com.quiltdata.quiltcore.workflows.WorkflowConfig;

import io.vertx.core.json.JsonObject;
import io.vertx.json.schema.JsonSchema;

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
    public void testWorkflows() {
//        SchemaRepository repo = SchemaRepository.create(new JsonSchemaOptions());

        try {
            Path path = Path.of("src", "test", "resources", "config.yml").toAbsolutePath();
            // S3PhysicalKey key = new S3PhysicalKey("quilt-dima", "config.yml", null);
            // WorkflowConfig config = WorkflowConfig.load(key);
            WorkflowConfig config = WorkflowConfig.load(new LocalPhysicalKey(path));

            assertNotEquals(null, config);

            System.out.println(config);

            // Files.newInputStream(path);

            // Path jsonPath = Path.of("src", "main", "resources", "config-1.schema.json");
            // String contents = Files.readString(jsonPath);

            // JsonObject obj = new JsonObject(contents);

            // JsonSchema schema = JsonSchema.of(obj);
        } catch (ConfigurationException e) {
            e.printStackTrace();
            fail();
        }
    }
}
