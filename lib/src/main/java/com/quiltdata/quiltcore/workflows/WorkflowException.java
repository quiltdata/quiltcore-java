package com.quiltdata.quiltcore.workflows;

/**
 * Represents an exception that can occur during a workflow execution.
 */
public class WorkflowException extends Exception {

    /**
     * Constructs a new WorkflowException with the specified detail message and cause.
     *
     * @param message the detail message (which is saved for later retrieval by the getMessage() method).
     * @param cause the cause (which is saved for later retrieval by the getCause() method).
     */
    public WorkflowException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new WorkflowException with the specified detail message.
     *
     * @param message the detail message (which is saved for later retrieval by the getMessage() method).
     */
    public WorkflowException(String message) {
        super(message);
    }
}
