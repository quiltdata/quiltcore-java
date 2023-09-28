package com.quiltdata.quiltcore;

import com.quiltdata.quiltcore.key.PhysicalKey;
import com.quiltdata.quiltcore.workflows.ConfigurationException;
import com.quiltdata.quiltcore.workflows.WorkflowConfig;

public class Registry {
    private final PhysicalKey names;
    private final PhysicalKey versions;
    private final PhysicalKey workflowConfigPath;

    public Registry(PhysicalKey root) {
        names = root.resolve(".quilt/named_packages");
        versions = root.resolve(".quilt/packages");
        workflowConfigPath = root.resolve(".quilt/workflows/config.yml");
    }

    public Namespace getNamespace(String name) {
        return new Namespace(this, name, names.resolve(name), versions);
    }

    public WorkflowConfig getWorkflowConfig() throws ConfigurationException {
        return WorkflowConfig.load(workflowConfigPath);
    }
}
