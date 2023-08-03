package com.quiltdata.quiltcore;

import com.quiltdata.quiltcore.key.PhysicalKey;

public class Registry {
    private PhysicalKey names;
    private PhysicalKey versions;

    public Registry(PhysicalKey root) {
        names = root.resolve(".quilt/named_packages");
        versions = root.resolve(".quilt/packages");
    }

    public Namespace getNamespace(String name) {
        return new Namespace(name, names.resolve(name), versions);
    }
}
