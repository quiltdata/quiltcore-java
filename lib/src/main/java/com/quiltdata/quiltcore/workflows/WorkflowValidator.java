package com.quiltdata.quiltcore.workflows;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.quiltdata.quiltcore.Entry;

import io.vertx.json.schema.OutputUnit;
import io.vertx.json.schema.Validator;
/**
 * This class represents a WorkflowValidator.
 * It is responsible for validating workflows.
 */
public class WorkflowValidator {
    /**
     * The data to store.
     */
    private final JsonNode dataToStore;
    
    /**
     * Indicates whether a message is required.
     */
    private final boolean isMessageRequired;
    
    /**
     * The pattern for package names.
     */
    private final Pattern pkgNamePattern;
    
    /**
     * The validator for metadata.
     */
    private final Validator metadataValidator;
    
    /**
     * The validator for entries.
     */
    private final Validator entriesValidator;

    /**
     * Constructs a WorkflowValidator object.
     *
     * @param dataToStore The data to store.
     * @param isMessageRequired Indicates whether a message is required.
     * @param pkgNamePattern The pattern for package names.
     * @param metadataValidator The validator for metadata.
     * @param entriesValidator The validator for entries.
     */
    public WorkflowValidator(
        JsonNode dataToStore,
        boolean isMessageRequired,
        Pattern pkgNamePattern,
        Validator metadataValidator,
        Validator entriesValidator
    ) {
        this.dataToStore = dataToStore;
        this.isMessageRequired = isMessageRequired;
        this.pkgNamePattern = pkgNamePattern;
        this.metadataValidator = metadataValidator;
        this.entriesValidator = entriesValidator;
    }

    /**
     * Validates the workflow.
     *
     * @param name The name of the workflow.
     * @param entries The entries of the workflow.
     * @param metadata The metadata of the workflow.
     * @param message The message of the workflow.
     * @throws WorkflowException If the workflow fails validation.
     */
    public void validate(String name, Map<String, Entry> entries, ObjectNode metadata, String message) throws WorkflowException {
        ObjectMapper mapper = new ObjectMapper();

        validateName(name);
        validateEntries(mapper, entries);
        validateMetadata(mapper, metadata);
        validateMessage(message);
    }

    /**
     * Validates the name of the workflow.
     *
     * @param name The name of the workflow.
     * @throws WorkflowException If the name doesn't match the required pattern.
     */
    private void validateName(String name) throws WorkflowException {
        if (pkgNamePattern != null) {
            Matcher m = pkgNamePattern.matcher(name);
            if (!m.find()) {
                throw new WorkflowException("Package name doesn't match required pattern");
            }
        }
    }

    /**
     * Validates the message of the workflow.
     *
     * @param message The message of the workflow.
     * @throws WorkflowException If the message is required but not provided.
     */
    private void validateMessage(String message) throws WorkflowException {
        if (isMessageRequired && (message == null || message.isEmpty())) {
            throw new WorkflowException("Commit message is required by workflow, but none was provided");
        }
    }

    /**
     * Creates an entry for validation.
     *
     * @param mapper The ObjectMapper instance.
     * @param logicalKey The logical key of the entry.
     * @param entry The entry to be validated.
     * @return The entry for validation.
     */
    private static Map<String, Object> entryForValidation(ObjectMapper mapper, String logicalKey, Entry entry) {
        try {
            return Map.of(
                "logical_key", logicalKey,
                "size", entry.getSize(),
                "meta", mapper.treeToValue(entry.getMetadata(), Object.class)
            );
        } catch (JsonProcessingException e) {
            // Should never happen.
            throw new RuntimeException(e);
        }
    }

    /**
     * Validates the entries of the workflow.
     *
     * @param mapper The ObjectMapper instance.
     * @param entries The entries of the workflow.
     * @throws WorkflowException If the entries fail validation.
     */
    private void validateEntries(ObjectMapper mapper, Map<String, Entry> entries) throws WorkflowException {
        if (entriesValidator == null) {
            return;
        }

        var entriesForValidation = entries.entrySet()
            .stream()
            .map(entry -> entryForValidation(mapper, entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());

        OutputUnit output = entriesValidator.validate(entriesForValidation);
        if (!output.getValid()) {
            throw new WorkflowException("Package entries failed validation");
        }
    }

    /**
     * Validates the metadata of the workflow.
     *
     * @param mapper The ObjectMapper instance.
     * @param metadata The metadata of the workflow.
     * @throws WorkflowException If the metadata fails validation.
     */
    private void validateMetadata(ObjectMapper mapper, ObjectNode metadata) throws WorkflowException {
        if (metadataValidator == null) {
            return;
        }

        JsonNode userMeta = metadata.get("user_meta");

        try {
            OutputUnit output = metadataValidator.validate(mapper.treeToValue(userMeta, Object.class));
            if (!output.getValid()) {
                throw new WorkflowException("Metadata failed validation");
            }
        } catch (JsonProcessingException e) {
            // Should never happen.
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the data to store.
     *
     * @return The data to store.
     */
    public JsonNode getDataToStore() {
        return dataToStore.deepCopy();
    }
}
