package com.quiltdata.quiltcore.workflows;

/**
 * This exception is thrown when there is an error in the configuration of a workflow.
 */
public class ConfigurationException extends Exception {
    /**
     * Constructs a new ConfigurationException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the {@link #getCause()} method)
     */
    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new ConfigurationException with the specified detail message.
     *
     * @param message the detail message
     */
    public ConfigurationException(String message) {
        super(message);
    }
}
