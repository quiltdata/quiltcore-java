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

public class WorkflowValidator {
    private JsonNode dataToStore;
    private boolean isMessageRequired;
    private Pattern pkgNamePattern;
    private Validator metadataValidator;
    private Validator entriesValidator;

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

    public void validate(String name, Map<String, Entry> entries, ObjectNode metadata, String message) throws WorkflowException {
        ObjectMapper mapper = new ObjectMapper();

        validateName(name);
        validateEntries(mapper, entries);
        validateMetadata(mapper, metadata);
        validateMessage(message);
    }

    private void validateName(String name) throws WorkflowException {
        if (pkgNamePattern != null) {
            Matcher m = pkgNamePattern.matcher(name);
            if (!m.find()) {
                throw new WorkflowException("Package name doesn't match required pattern");
            }
        }
    }

    private void validateMessage(String message) throws WorkflowException {
        if (isMessageRequired && (message == null || message.isEmpty())) {
            throw new WorkflowException("Commit message is required by workflow, but none was provided");
        }
    }

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

    public JsonNode getDataToStore() {
        return dataToStore.deepCopy();
    }
}
