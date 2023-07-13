package com.quiltdata.quiltcore;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

import static org.junit.Assert.*;
import org.junit.Test;


public class RegistryTest {
    @Test
    public void testS3() {
        try {
            Path p = Path.of(new URI("s3://quilt-example/"));

            Registry r = new Registry(p);
            Namespace n = r.getNamespace("examples/metadata");
            Manifest m = n.getManifest("latest");

            Entry e = m.getEntry("README.md");

            byte[] data = e.getBytes();
            String readme = new String(data);

            System.out.println(readme);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        } catch (URISyntaxException e) {
            e.printStackTrace();
            fail();
        }
    }
}
