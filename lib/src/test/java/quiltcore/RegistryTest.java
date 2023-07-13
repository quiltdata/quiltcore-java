package quiltcore;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import nextflow.plugin.CustomPluginManager;
import nextflow.plugin.Plugins;
import nextflow.file.FileHelper;

import org.junit.Test;
import org.junit.BeforeClass;

public class RegistryTest {

	@BeforeClass
	public static void initPlugins() {
		Path path = Paths.get("plugins");
		Path root = path.toAbsolutePath().normalize();

		CustomPluginManager manager = new CustomPluginManager(root);
		manager.loadPlugins();

		Plugins.init(root, "prod", manager);
	}

	@Test
	public void test() {
		try {
			Path p = FileHelper.asPath("s3://quilt-example/");
			Registry r = new Registry(p);
			Namespace n = r.get("examples/metadata");
			Manifest m = n.get("latest");

			Entry e = m.get("README.md");

			byte[] data = e.get();
			String readme = new String(data);

			System.out.println(readme);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void wtf() {
		try {
			Path p = FileHelper.asPath("s3://quilt-example/README.md");
			String s = Files.readString(p);
			System.out.println(s);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
