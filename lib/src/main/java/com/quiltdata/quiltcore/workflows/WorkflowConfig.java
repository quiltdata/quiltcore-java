package com.quiltdata.quiltcore.workflows;

import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.PhysicalKey;

import io.vertx.core.json.JsonObject;
import io.vertx.json.schema.Draft;
import io.vertx.json.schema.JsonSchema;
import io.vertx.json.schema.JsonSchemaOptions;
import io.vertx.json.schema.OutputUnit;
import io.vertx.json.schema.Validator;

public class WorkflowConfig {
    public static final ConfigDataVersion CONFIG_DATA_VERSION = new ConfigDataVersion(1, 1, 0);

    private static final Map<String, Draft> SUPPORTED_META_SCHEMAS = Map.of(
        "http://json-schema.org/draft-07/schema#", Draft.DRAFT7
    );

    private static final Validator configValidator;

    static {
        try {
            byte[] bytes = WorkflowConfig.class.getResourceAsStream("/config-1.schema.json").readAllBytes();
            JsonObject obj = new JsonObject(new String(bytes));
            JsonSchema configSchema = JsonSchema.of(obj);
            JsonSchemaOptions options = new JsonSchemaOptions()
                .setBaseUri("https://quiltdata.com/")  // TODO: remove it; not actually used.
                .setDraft(Draft.DRAFT7);
            configValidator = Validator.create(configSchema, options);
        } catch (IOException e) {
            // Should never happen.
            throw new RuntimeException(e);
        }
    }

    private static class SchemaInfo {
        public final Validator validator;
        public final PhysicalKey physicalKey;

        public SchemaInfo(Validator validator, PhysicalKey physicalKey) {
            this.validator = validator;
            this.physicalKey = physicalKey;
        }
    }

    private JsonNode config;
    private PhysicalKey physicalKey;
    private Map<String, SchemaInfo> loadedSchemas;

    public WorkflowConfig(JsonNode config, PhysicalKey physicalKey) {
        this.config = config;
        this.physicalKey = physicalKey;
        loadedSchemas = new HashMap<>();
    }

    public static WorkflowConfig load(PhysicalKey physicalKey) throws ConfigurationException {
        byte[] data;
        PhysicalKey effectivePhysicalKey;
        try {
            var response = physicalKey.open();
            data = response.inputStream.readAllBytes();
            effectivePhysicalKey = response.effectivePhysicalKey;
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw new ConfigurationException("Couldn't load workflows config", e);
        }
        if (data.length == 0) {
            return null;
        }

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode node;
        Object value;
        try {
            node = mapper.readTree(data);
            value = mapper.treeToValue(node, Object.class);
        } catch (IOException e) {
            throw new ConfigurationException("Couldn't parse workflows config as YAML", e);
        }
        OutputUnit output = configValidator.validate(value);

        if (!output.getValid()) {
            throw new ConfigurationException("Workflows config failed validation");
        }

        checkVersion(node);

        return new WorkflowConfig(node, effectivePhysicalKey);
    }

    private static void checkVersion(JsonNode node) throws ConfigurationException {
        JsonNode versionNode = node.get("version");
        if (versionNode.isObject()) {
            versionNode = versionNode.get("base");
        }

        String versionStr = versionNode.asText();
        ConfigDataVersion version = ConfigDataVersion.parse(versionStr);
        if (CONFIG_DATA_VERSION.compareTo(version) < 0) {
            throw new ConfigurationException("Version " + version + " is not supported");
        }
    }

    public String getDefaultWorkflow() {
        JsonNode node = config.get("default_workflow");
        return node == null ? "" : node.asText();
    }

    public WorkflowValidator getWorkflowValidator(String workflow) throws WorkflowException, ConfigurationException {
        JsonNode workflowData;

        if (workflow == null) {
            workflow = getDefaultWorkflow();
        }

        if (workflow.isEmpty()) {
            JsonNode requiredNode = config.get("is_workflow_required");
            if (requiredNode == null || requiredNode.asBoolean(true)) {
                throw new WorkflowException("Workflow required, but none specified.");
            }
            workflowData = JsonNodeFactory.instance.objectNode();
        } else {
            workflowData = config.get("workflows").get(workflow);
            if (workflowData == null) {
                throw new WorkflowException("There is no '" + workflow + "' workflow in config.");
            }
        }

        JsonNode pkgNamePatternNode = workflowData.get("handle_pattern");
        Pattern pkgNamePattern = pkgNamePatternNode != null ? Pattern.compile(pkgNamePatternNode.asText()) : null;

        JsonNode metadataSchemaId = workflowData.get("metadata_schema");
        Validator metadataValidator = metadataSchemaId != null ? makeValidatorFromSchema(metadataSchemaId.asText()) : null;

        JsonNode entriesSchemaId = workflowData.get("entries_schema");
        Validator entriesValidator = entriesSchemaId != null ? makeValidatorFromSchema(entriesSchemaId.asText()) : null;

        JsonNode isMessageRequiredNode = workflowData.get("is_message_required");
        boolean isMessageRequired = isMessageRequiredNode != null ? isMessageRequiredNode.asBoolean(false) : false;

        return new WorkflowValidator(isMessageRequired, pkgNamePattern, metadataValidator, entriesValidator);
    }

    private Validator makeValidatorFromSchema(String schemaId) throws ConfigurationException {
        PhysicalKey schemaPhysicalKey = getPhysicalKeyForSchemaId(schemaId);
        SchemaInfo info = loadedSchemas.get(schemaPhysicalKey.toString());
        if (info != null) {
            return info.validator;
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaNode;
        PhysicalKey schemaEffectivePhysicalKey;
        try {
            var response = schemaPhysicalKey.open();
            schemaNode = mapper.readTree(response.inputStream);
            schemaEffectivePhysicalKey = response.effectivePhysicalKey;
        } catch (StreamReadException e) {
            throw new ConfigurationException("Couldn't parse " + schemaPhysicalKey + " as JSON", e);
        } catch (IOException e) {
            throw new ConfigurationException("Couldn't load schema at " + schemaPhysicalKey, e);
        }

        Draft draft = Draft.DRAFT7;
        JsonNode metaSchemaNode = schemaNode.get("$schema");
        if (metaSchemaNode != null) {
            if (!metaSchemaNode.isTextual()) {
                throw new ConfigurationException("$schema must be a string");
            }
            draft = SUPPORTED_META_SCHEMAS.get(metaSchemaNode.asText());
            if (draft == null) {
                throw new ConfigurationException("Unsupported meta-schema: " + metaSchemaNode.asText());
            }
        }

        JsonSchema schema = JsonSchema.of(JsonObject.mapFrom(schemaNode));
        JsonSchemaOptions options = new JsonSchemaOptions()
            .setBaseUri("https://quiltdata.com/")  // TODO: remove it; not actually used.
            .setDraft(draft);
        Validator validator = Validator.create(schema, options);

        info = new SchemaInfo(validator, schemaEffectivePhysicalKey);
        loadedSchemas.put(schemaPhysicalKey.toString(), info);
        return validator;
    }

    private PhysicalKey getPhysicalKeyForSchemaId(String schemaId) throws ConfigurationException {
        JsonNode schema = config.get("schemas").get(schemaId);
        if (schema == null) {
            throw new ConfigurationException("There is no '" + schemaId + "' in schemas.");
        }
        String schemaUrl = schema.get("url").asText();
        PhysicalKey schemaPk;
        try {
            schemaPk = PhysicalKey.fromUri(new URI(schemaUrl));
        } catch (Exception e) {
            throw new ConfigurationException("Couldn't parse URL: " + schemaUrl, e);
        }

        if (schemaPk instanceof LocalPhysicalKey && !(physicalKey instanceof LocalPhysicalKey)) {
            throw new ConfigurationException("Local schema " + schemaPk + " can't be used on the remote registry.");
        }

        return schemaPk;
    }
}
