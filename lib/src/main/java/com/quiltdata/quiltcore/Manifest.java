package com.quiltdata.quiltcore;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pivovarit.function.ThrowingFunction;
import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.PhysicalKey;
import com.quiltdata.quiltcore.key.S3PhysicalKey;
import com.quiltdata.quiltcore.ser.PythonDoubleSerializer;
import com.quiltdata.quiltcore.workflows.ConfigurationException;
import com.quiltdata.quiltcore.workflows.WorkflowConfig;
import com.quiltdata.quiltcore.workflows.WorkflowException;
import com.quiltdata.quiltcore.workflows.WorkflowValidator;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;
import software.amazon.awssdk.utils.BinaryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Manifest class represents a collection of entries and metadata associated with a dataset.
 * It provides methods for building, reading, and serializing manifests.
 */
public class Manifest {
    /**
     * The version of the manifest.
     */
    public static final String VERSION = "v0";

    private static final ObjectMapper TOP_HASH_MAPPER;

    private static final Logger logger = LoggerFactory.getLogger(Manifest.class);

    static {
        TOP_HASH_MAPPER = new ObjectMapper();
        SimpleModule sm = new SimpleModule();
        sm.addSerializer(Double.class, new PythonDoubleSerializer());
        TOP_HASH_MAPPER.registerModule(sm);
        TOP_HASH_MAPPER.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    }

    /**
     * Represents a builder for creating a {@link Manifest} object.
     */
    public static class Builder {
        private SortedMap<String, Entry> entries;
        private ObjectNode metadata;

        /**
         * Constructs a new instance of the {@code Builder} class.
         * Initializes the entries map and sets the metadata to null.
         */
        public Builder() {
            entries = new TreeMap<>();
            metadata = null;
        }

        /**
         * Sets the metadata for the manifest.
         *
         * @param metadata The metadata to set.
         */
        public void setMetadata(ObjectNode metadata) {
            this.metadata = metadata;
        }

        /**
         * Adds an entry to the manifest.
         *
         * @param key   The key of the entry.
         * @param entry The entry to add.
         */
        public void addEntry(String key, Entry entry) {
            entries.put(key, entry);
        }

        /**
         * Builds a {@link Manifest} object using the provided entries and metadata.
         *
         * @return The built {@link Manifest} object.
         */
        public Manifest build() {
            logger.debug("Building manifest with {} entries", entries.size());
            return new Manifest(
                Collections.unmodifiableSortedMap(entries),
                metadata == null ? JsonNodeFactory.instance.objectNode().put("version", VERSION) : metadata.deepCopy()
            );
        }
    }

    private final SortedMap<String, Entry> entries;
    private final ObjectNode metadata;

    private Manifest(SortedMap<String, Entry> entries, ObjectNode metadata) {
        this.entries = entries;
        this.metadata = metadata;
    }

    /**
     * Returns a {@link Builder} class for creating instances of the Manifest class.
     * 
     * @return A new instance of the {@link Builder} class.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a {@link Manifest} object from a file.
     * 
     * @param path The path to the file to create the manifest from.
     * @return The created {@link Manifest} object.
     * @throws IOException If an I/O error occurs.
     * @throws URISyntaxException If the URI is invalid.
     */
    public static Manifest createFromFile(PhysicalKey path) throws IOException, URISyntaxException {
        Builder builder = builder();

        ObjectMapper mapper = new ObjectMapper();

        logger.debug("Reading manifest from {}", path);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(path.getInputStream()))) {
            String header = reader.readLine();
            JsonNode node = mapper.readTree(header);
            if (!node.isObject()) {
                throw new IOException("Invalid manifest metadata: " + node);
            }
            ObjectNode manifestMeta = (ObjectNode)node;
            String version = manifestMeta.get("version").asText();
            if (!version.equals(VERSION)) {
                throw new IOException("Unsupported manifest version: " + version);
            }
            builder.setMetadata(manifestMeta);

            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode row = mapper.readTree(line);

                String logicalKey = row.get("logical_key").asText();
                JsonNode physicalKeysNode = row.get("physical_keys");
                if (physicalKeysNode == null) {
                    // TODO: Handle directory-level metadata?
                    continue;
                }
                String physicalKeyString = physicalKeysNode.get(0).asText();
                PhysicalKey physicalKey = PhysicalKey.fromUri(new URI(physicalKeyString));
                long size = row.get("size").asLong();
                JsonNode hashNode = row.get("hash");
                Entry.HashType hashType = Entry.HashType.valueOf(hashNode.get("type").asText());
                String hashValue = hashNode.get("value").asText();
                JsonNode meta = row.get("meta");
                if (meta == null) {
                    // leave it as is
                } else if (meta.isNull()) {
                    meta = null;
                } else if (!meta.isObject()) {
                    throw new IOException("Invalid entry metadata: " + node);
                }

                Entry entry = new Entry(physicalKey, size, new Entry.Hash(hashType, hashValue), (ObjectNode)meta);
                builder.addEntry(logicalKey, entry);
            }
        }

        return builder.build();
    }

    /**
     * Serializes the manifest to an output stream.
     * 
     * @param out The output stream to serialize the manifest to.
     * @throws IOException If an I/O error occurs.
     */
    public void serializeToOutputStream(OutputStream out) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        String version = metadata.get("version").asText();
        if (!version.equals(VERSION)) {
            throw new IOException("Unsupported manifest version: " + version);
        }

        logger.debug("Serializing manifest metadata {}", metadata);
        out.write(mapper.writeValueAsBytes(metadata));
        out.write('\n');

        logger.debug("Serializing manifest with {} entries", entries.size());
        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            String logicalKey = e.getKey();
            Entry entry = e.getValue();

            if (entry.getHash() == null) {
                throw new IOException("Cannot serialize entries without hashes!");
            }

            String physicalKey = entry.getPhysicalKey().toUri().toString();

            ObjectNode row = mapper.createObjectNode();
            row.put("logical_key", logicalKey);
            row.putArray("physical_keys").add(physicalKey);
            row.put("size", entry.getSize());
            row.putPOJO("hash", entry.getHash());
            row.set("meta", entry.getMetadata());

            out.write(mapper.writeValueAsBytes(row));
            out.write('\n');
        }
    }

    /**
     * Calculates the top hash of the manifest.
     * 
     * @return The top hash of the manifest.
     * @throws IOException If an I/O error occurs.
     */
    public String calculateTopHash() throws IOException {
        MessageDigest digest;

        logger.debug("Calculating top hash for manifest with {} entries", entries.size());
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // This should never happen, and if it does happen, there's nothing we can do.
            // Let's not require the caller to handle it.
            throw new RuntimeException(e);
        }

        /* Everything has to be serialized exactly as quilt3 in order for the hash to match:
         * - No spaces (already the default)
         * - Sorted object keys
         * - Floating point numbers formatted the same as in Python
         *
         * To do this, we convert existing JSON objects to native Java objects first,
         * and use a custom Double serializer (otherwise, serialization overrides don't work).
         * Inefficient, but hopefully correct.
         */

        Object metadataObj = TOP_HASH_MAPPER.treeToValue(metadata, Object.class);
        byte[] headerBytes = TOP_HASH_MAPPER.writeValueAsBytes(metadataObj);
        digest.update(headerBytes);

        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            String logicalKey = e.getKey();
            Entry entry = e.getValue();

            if (entry.getHash() == null) {
                throw new IOException("Cannot calculate top hash for entries without hashes!");
            }

            Map<String, Object> row = Map.of(
                "logical_key", logicalKey,
                "hash", entry.getHash(),
                "size", entry.getSize(),
                "meta", TOP_HASH_MAPPER.treeToValue(entry.getMetadata(), Object.class)
            );

            byte[] bytes = TOP_HASH_MAPPER.writeValueAsBytes(row);
            digest.update(bytes);
        }

        return BinaryUtils.toHex(digest.digest());
    }

    /** 
     * Returns the entries in the manifest.
     * 
     * @return The entries in the manifest.
     */
    public SortedMap<String, Entry> getEntries() {
        return entries;
    }

    /**
     * Returns the metadata in the manifest.
     * @return {@link ObjectNode}
     */
    public ObjectNode getMetadata() {
        return metadata.deepCopy();
    }

    private Path resolveDest(Path dest, String logicalKey) throws IOException {
        Path entryDest = dest.resolve(logicalKey).normalize();
        if (!entryDest.startsWith(dest)) {
            throw new IOException("Invalid logical key: " + logicalKey);
        }
        Files.createDirectories(entryDest.getParent());
        return entryDest;
    }

    /**
     * Installs the manifest to the specified destination.
     * 
     * @param dest The destination to install the manifest to.
     * @throws IOException If an I/O error occurs.
     */
    public void install(Path dest) throws IOException {
        // TODO: save the manifest to the local registry?

        Map<String, List<Map.Entry<String, Entry>>> entriesByBucket =
            entries.entrySet().stream().collect(Collectors.groupingBy(
                ThrowingFunction.sneaky(entry -> {
                    PhysicalKey pk = entry.getValue().getPhysicalKey();
                    if (!(pk instanceof S3PhysicalKey)) {
                        throw new IOException("Expected s3 paths, but got a local path: " + pk);
                    }
                    return ((S3PhysicalKey)pk).getBucket();
                })
            ));

        // Ideally, we would parallelize all downloads, but S3TransferManager is per-region.
        // But, a single-bucket manifest is the common case, so group all paths by buckets,
        // then parallelize everything within the bucket.
        for (Map.Entry<String, List<Map.Entry<String, Entry>>> e : entriesByBucket.entrySet()) {
            String bucket = e.getKey();
            List<Map.Entry<String, Entry>> bucketEntries = e.getValue();

            S3AsyncClient s3;
            logger.debug("Creating S3 client for bucket: {}", bucket);
            try {
                s3 = S3ClientStore.getAsyncClient(bucket);
            } catch (S3Exception ex) {
                throw new IOException("Install failed", ex.getCause());
            }

            logger.debug("Building transfer manager for bucket: {}", bucket);
            try(
                S3TransferManager transferManager =
                    S3TransferManager.builder()
                        .s3Client(s3)
                        .build();
            ) {
                List<CompletableFuture<CompletedFileDownload>> futures = new ArrayList<>(bucketEntries.size());

                for (Map.Entry<String, Entry> e2 : bucketEntries) {
                    String logicalKey = e2.getKey();
                    Entry entry = e2.getValue();
                    String key = ((S3PhysicalKey)entry.getPhysicalKey()).getKey();

                    Path entryDest = resolveDest(dest, logicalKey);

                    logger.debug("Downloading file from bucket: {}, key: {}", bucket, key);
                    DownloadFileRequest downloadFileRequest =
                        DownloadFileRequest.builder()
                            .getObjectRequest(b -> b.bucket(bucket).key(key))
                            .addTransferListener(LoggingTransferListener.create())
                            .destination(entryDest)
                            .build();

                    FileDownload downloadFile = transferManager.downloadFile(downloadFileRequest);
                    futures.add(downloadFile.completionFuture());
                }

                for (CompletableFuture<CompletedFileDownload> future : futures) {
                    future.join();
                }
            } catch (CompletionException ex) {
                throw new IOException("Install failed", ex.getCause());
            }
        }
    }

    private JsonNode validate(Namespace namespace, String message, String workflow) throws ConfigurationException, WorkflowException {
        WorkflowConfig config = namespace.getRegistry().getWorkflowConfig();
        if (config == null) {
            if (workflow == null) {
                return null;
            }
            throw new WorkflowException("Workflow is specified, but no workflows config exists");
        }

        WorkflowValidator validator = config.getWorkflowValidator(workflow);
        validator.validate(namespace.getName(), entries, metadata, message);
        return validator.getDataToStore();
    }

    /**
     * Pushes the manifest to the specified namespace.
     * 
     * @param namespace The namespace to push the manifest to.
     * @param message The message to associate with the push.
     * @param workflow The workflow to run on the pushed data.
     * @return The pushed {@link Manifest}
     * @throws IOException If an I/O error occurs.
     * @throws ConfigurationException If a configuration error occurs.
     * @throws WorkflowException If a workflow error occurs.
     */
    public Manifest push(Namespace namespace, String message, String workflow) throws IOException, ConfigurationException, WorkflowException {
        PhysicalKey namespacePath = namespace.getPath();
        if (!(namespacePath instanceof S3PhysicalKey)) {
            throw new IOException("Only S3 namespace supported");
        }

        JsonNode workflowInfo = validate(namespace, message, workflow);

        S3PhysicalKey s3NamespacePath = (S3PhysicalKey)namespacePath;
        String destBucket = s3NamespacePath.getBucket();

        S3AsyncClient s3;
        logger.debug("Creating S3 client for bucket: {}", destBucket);
        try {
            s3 = S3ClientStore.getAsyncClient(destBucket);
        } catch (S3Exception ex) {
            throw new IOException("Push failed", ex.getCause());
        }

        Map<String, Entry> entriesWithHashes = entries.entrySet()
            .stream()
            .parallel()
            .map(ThrowingFunction.sneaky(entry -> Map.entry(entry.getKey(), entry.getValue().withHash())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Builder builder = builder();
        ObjectNode newMetadata = getMetadata();
        newMetadata.put("message", message);
        if (workflowInfo != null) {
            newMetadata.set("workflow", workflowInfo);
        } else {
            newMetadata.remove("workflow");
        }
        builder.setMetadata(newMetadata);

        logger.debug("Building transfer manager for bucket: {}", destBucket);
        try(
            S3TransferManager transferManager =
                S3TransferManager.builder()
                    .s3Client(s3)
                    .build();
        ) {
            List<Map.Entry<String, CompletableFuture<CompletedFileUpload>>> futures =
                new ArrayList<>(entriesWithHashes.size());

            for (Map.Entry<String, Entry> e : entriesWithHashes.entrySet()) {
                String logicalKey = e.getKey();
                Entry entry = e.getValue();

                String destPath = namespace.getName() + "/" + logicalKey;

                PhysicalKey src = entry.getPhysicalKey();
                if (!(src instanceof LocalPhysicalKey)) {
                    throw new IOException("Only local physical keys supported");
                }
                LocalPhysicalKey localSrc = (LocalPhysicalKey)src;

                Path sourcePath = Path.of(localSrc.getPath());
                if (!Files.exists(sourcePath)) {
                    throw new IOException("Source file does not exist: " + sourcePath);
                }

                UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                    .putObjectRequest(req -> req.bucket(destBucket).key(destPath))
                    .addTransferListener(LoggingTransferListener.create())
                    .source(sourcePath)
                    .build();

                logger.debug("Uploading file to bucket: {}, key: {}", destBucket, destPath);
                FileUpload uploadFile = transferManager.uploadFile(uploadFileRequest);
                futures.add(Map.entry(logicalKey, uploadFile.completionFuture()));
            }

            for (Map.Entry<String, CompletableFuture<CompletedFileUpload>> future : futures) {
                PutObjectResponse uploadResponse = future.getValue().join().response();

                String logicalKey = future.getKey();
                Entry origEntry = entriesWithHashes.get(logicalKey);
                String destPath = namespace.getName() + "/" + logicalKey;
                S3PhysicalKey dest = new S3PhysicalKey(destBucket, destPath, uploadResponse.versionId());
                builder.addEntry(logicalKey, new Entry(dest, origEntry.getSize(), origEntry.getHash(), origEntry.getMetadata()));
            }
        } catch (CompletionException ex) {
            throw new IOException("Push failed", ex.getCause());
        }

        Manifest newManifest = builder.build();

        String topHash = newManifest.calculateTopHash();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        newManifest.serializeToOutputStream(out);
        namespace.getVersions().resolve(topHash).putBytes(out.toByteArray());

        long unixTime = System.currentTimeMillis() / 1000L;
        namespace.getPath().resolve("" + unixTime).putBytes(topHash.getBytes(StandardCharsets.UTF_8));
        namespace.getPath().resolve("latest").putBytes(topHash.getBytes(StandardCharsets.UTF_8));

        return newManifest;
    }
}
