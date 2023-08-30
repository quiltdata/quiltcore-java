package com.quiltdata.quiltcore.workflows;

public class WorkflowException extends Exception {
    public WorkflowException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkflowException(String message) {
        super(message);
    }
}
