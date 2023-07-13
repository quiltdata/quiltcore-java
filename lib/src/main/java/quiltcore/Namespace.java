package quiltcore;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class Namespace {
	private Path path;
	private Path versions;

	public Namespace(Path path, Path versions) {
		this.path = path;
		this.versions = versions;
	}

	public String getHash(String tag) throws IOException {
		try (BufferedReader reader = Files.newBufferedReader(path.resolve(tag), StandardCharsets.UTF_8)) {
			String line = reader.readLine();
			reader.close();
			return line;
		}

		// return Files.readString(path.resolve(tag));
	}

	public Manifest get(String tag) throws IOException {
		String hash = getHash(tag);
		return new Manifest(versions.resolve(hash));
	}
}
