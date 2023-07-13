package quiltcore;

import java.nio.file.Path;

public class Registry {
	private Path names;
	private Path versions;

	public Registry(Path root) {
		names = root.resolve(".quilt/named_packages");
		versions = root.resolve(".quilt/packages");
	}
	
	public Namespace get(String name) {
		// TODO: name validation
		return new Namespace(names.resolve(name), versions);
	}
}
