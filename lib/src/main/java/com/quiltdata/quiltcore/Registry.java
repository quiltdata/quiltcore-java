package com.quiltdata.quiltcore;

import java.nio.file.Path;


public class Registry {
    private Path names;
    private Path versions;

    public Registry(Path root) {
        names = root.resolve(".quilt/named_packages");
        versions = root.resolve(".quilt/packages");
    }

    public Namespace getNamespace(String name) {
        return new Namespace(names.resolve(name), versions);
    }
}
