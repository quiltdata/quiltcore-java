package com.quiltdata.quiltcore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.quiltdata.quiltcore.ser.PythonDoubleSerializer;


public class SerializationTest {
    @Test
    public void testFloat() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule sm = new SimpleModule();
        sm.addSerializer(Double.class, new PythonDoubleSerializer());
        mapper.registerModule(sm);

        try {
            Path jsonPath = Path.of("src", "test", "resources", "python_floats.json");
            String original = Files.readString(jsonPath);

            JsonNode node = mapper.readTree(original);
            Object val = mapper.treeToValue(node, Object.class);
            String serialized = mapper.writeValueAsString(val);

            assertEquals(original, serialized);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }
}
