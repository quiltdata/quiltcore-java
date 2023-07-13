package quiltcore;

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
	private Path path;
	private Map<String, Entry> entries;

	public Manifest(Path path) throws IOException {
		this.path = path;
		entries = new HashMap<>();

		ObjectMapper mapper = new ObjectMapper();
		
		try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
			String header = reader.readLine();
			JsonNode node = mapper.readTree(header);
			String version = node.get("version").asText();
			if (!version.equals("v0")) {
				throw new IOException("Unsupported manifest version: " + version);
			}
			String line;
			while ((line = reader.readLine()) != null) {
				JsonNode row = mapper.readTree(line);

				Entry entry = new Entry();

				String logicalKey = row.get("logical_key").asText();
				entry.physicalKey = row.get("physical_keys").get(0).asText();
				entry.size = row.get("size").asLong();
				JsonNode hashNode = row.get("hash");
				entry.hash = new Entry.Hash();
				entry.hash.type = Entry.HashType.valueOf(hashNode.get("type").asText());
				entry.hash.value = hashNode.get("value").asText();

				entries.put(logicalKey, entry);
			}
		}
	}

	public Entry get(String logicalKey) {
		return entries.get(logicalKey);
	}

	public String toString() {
		return "Manifest[" + path + "]";
	}
}
