package com.quiltdata.quiltcore;

import com.quiltdata.quiltcore.key.PhysicalKey;
import com.quiltdata.quiltcore.workflows.ConfigurationException;
import com.quiltdata.quiltcore.workflows.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Registry class represents a registry of packages and namespaces in the Quilt Core library.
 * It provides methods to access and manipulate the registry.
 */
public class Registry {
    private static final Logger logger = LoggerFactory.getLogger(Registry.class);

    private final PhysicalKey names;
    private final PhysicalKey versions;
    private final PhysicalKey workflowConfigPath;

    /**
     * Constructs a new Registry object with the specified root physical key.
     *
     * @param root The root physical key of the registry.
     */
    public Registry(PhysicalKey root) {
        logger.info("Creating registry at root: {}", root);
        names = root.resolve(".quilt/named_packages");
        versions = root.resolve(".quilt/packages");
        workflowConfigPath = root.resolve(".quilt/workflows/config.yml");
    }

    /**
     * Returns the Namespace object associated with the specified name.
     *
     * @param name The name of the namespace.
     * @return The Namespace object.
     */
    public Namespace getNamespace(String name) {
        logger.debug("Getting namespace: {}", name);
        return new Namespace(this, name, names.resolve(name), versions);
    }

    /**
     * Returns the WorkflowConfig object representing the workflow configuration.
     *
     * @return The WorkflowConfig object.
     * @throws ConfigurationException If there is an error loading the workflow configuration.
     */
    public WorkflowConfig getWorkflowConfig() throws ConfigurationException {
        return WorkflowConfig.load(workflowConfigPath);
    }
}
