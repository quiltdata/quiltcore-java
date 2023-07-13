package com.quiltdata.quiltcore;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class Manifest {
    private Map<String, Entry> entries;

    public Manifest(Path path) throws IOException {
        entries = new HashMap<>();

        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            JsonNode node = mapper.readTree(header);
            String version = node.get("version").asText();
            if (!version.equals("v0")) {
                throw new IOException("Unsupported manifest version: " + version);
            }
            // TODO: package metadata

            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode row = mapper.readTree(line);

                String logicalKey = row.get("logical_key").asText();
                String physicalKey = row.get("physical_keys").get(0).asText();
                long size = row.get("size").asLong();
                JsonNode hashNode = row.get("hash");
                Entry.HashType hashType = Entry.HashType.valueOf(hashNode.get("type").asText());
                String hashValue = hashNode.get("value").asText();
                // TODO: entry metadata

                Entry entry = new Entry(physicalKey, size, new Entry.Hash(hashType, hashValue));
                entries.put(logicalKey, entry);
            }
        }
    }

    public Entry getEntry(String logicalKey) {
        return entries.get(logicalKey);
    }
}
