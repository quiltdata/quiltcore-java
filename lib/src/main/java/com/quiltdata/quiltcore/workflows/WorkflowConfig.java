package com.quiltdata.quiltcore.workflows;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the configuration for workflows in Quilt.
 */
public class WorkflowConfig {
    /**
     * The version of the configuration data.
     */
    public static final ConfigDataVersion CONFIG_DATA_VERSION = new ConfigDataVersion(1, 1, 0);

    private static final Logger logger = LoggerFactory.getLogger(WorkflowConfig.class);

    private static final Map<String, Draft> SUPPORTED_META_SCHEMAS = Map.of(
        "http://json-schema.org/draft-07/schema#", Draft.DRAFT7
    );

    private static final Validator configValidator;

    static {
        logger.info("Loading WorkflowConfig schema");
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

    private final JsonNode config;
    private final PhysicalKey physicalKey;
    private final Map<String, SchemaInfo> loadedSchemas;
    private final Map<String, SchemaInfo> loadedSchemasById;

    /**
     * Constructs a new WorkflowConfig instance.
     *
     * @param config The JSON configuration node.
     * @param physicalKey The physical key associated with the configuration.
     */
    public WorkflowConfig(JsonNode config, PhysicalKey physicalKey) {
        this.config = config;
        this.physicalKey = physicalKey;
        loadedSchemas = new HashMap<>();
        loadedSchemasById = new HashMap<>();
    }

    /**
     * Loads the WorkflowConfig from the specified physical key.
     *
     * @param physicalKey The physical key to load the configuration from.
     * @return The loaded WorkflowConfig instance, or null if the configuration is not found.
     * @throws ConfigurationException If there is an error loading or parsing the configuration.
     */
    public static WorkflowConfig load(PhysicalKey physicalKey) throws ConfigurationException {
        byte[] data;
        PhysicalKey effectivePhysicalKey;

        logger.info("Loading workflows config from {}", physicalKey);
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

        logger.debug("Parsing workflows config from {}", data);
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

    /**
     * Gets the default workflow specified in the configuration.
     *
     * @return The default workflow, or an empty string if not specified.
     */
    public String getDefaultWorkflow() {
        JsonNode node = config.get("default_workflow");
        return node == null ? "" : node.asText();
    }

    /**
     * Gets the WorkflowValidator for the specified workflow.
     *
     * @param workflow The name of the workflow. If null, the default workflow will be used.
     * @return The WorkflowValidator instance.
     * @throws WorkflowException If there is an error with the workflow.
     * @throws ConfigurationException If there is an error with the configuration.
     */
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
        logger.info("metadataSchemaId={}", metadataSchemaId);
        Validator metadataValidator = metadataSchemaId != null ? makeValidatorFromSchema(metadataSchemaId.asText()) : null;

        JsonNode entriesSchemaId = workflowData.get("entries_schema");
        logger.info("entriesSchemaId={}", entriesSchemaId);
        Validator entriesValidator = entriesSchemaId != null ? makeValidatorFromSchema(entriesSchemaId.asText()) : null;

        JsonNode isMessageRequiredNode = workflowData.get("is_message_required");
        boolean isMessageRequired = isMessageRequiredNode != null && isMessageRequiredNode.asBoolean(false);

        var dataToStore = JsonNodeFactory.instance.objectNode()
            .put("id", workflow.isEmpty() ? null : workflow)
            .put("config", physicalKey.toString());
        if (!loadedSchemasById.isEmpty()) {
            var schemaNode = JsonNodeFactory.instance.objectNode();
            for (var entry : loadedSchemasById.entrySet()) {
                schemaNode.put(entry.getKey(), entry.getValue().physicalKey.toString());
            }
            dataToStore.set("schemas", schemaNode);
        }

        return new WorkflowValidator(dataToStore, isMessageRequired, pkgNamePattern, metadataValidator, entriesValidator);
    }

    private Validator makeValidatorFromSchema(String schemaId) throws ConfigurationException {
        SchemaInfo info = loadedSchemasById.get(schemaId);
        if (info != null) {
            return info.validator;
        }

        PhysicalKey schemaPhysicalKey = getPhysicalKeyForSchemaId(schemaId);
        info = loadedSchemas.get(schemaPhysicalKey.toString());
        if (info != null) {
            loadedSchemasById.put(schemaId, info);
            return info.validator;
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode schemaNode;
        PhysicalKey schemaEffectivePhysicalKey;

        logger.info("Loading schema from {}", schemaPhysicalKey);
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
        logger.info("Creating validator for schema: {}", schema);
        Validator validator = Validator.create(schema, options);

        info = new SchemaInfo(validator, schemaEffectivePhysicalKey);
        loadedSchemasById.put(schemaId, info);
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

        logger.debug("Resolving schema URL: {}", schemaUrl);
        try {
            schemaPk = PhysicalKey.fromUri(new URI(schemaUrl));
        } catch (IllegalArgumentException | URISyntaxException e) {
            throw new ConfigurationException("Couldn't parse URL: " + schemaUrl, e);
        }

        if (schemaPk instanceof LocalPhysicalKey && !(physicalKey instanceof LocalPhysicalKey)) {
            throw new ConfigurationException("Local schema " + schemaPk + " can't be used on the remote registry.");
        }

        return schemaPk;
    }
}
