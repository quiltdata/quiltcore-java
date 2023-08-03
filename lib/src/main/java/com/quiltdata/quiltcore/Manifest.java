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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.PhysicalKey;
import com.quiltdata.quiltcore.key.S3PhysicalKey;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;
import software.amazon.awssdk.utils.BinaryUtils;


public class Manifest {
    public static final String VERSION = "v0";

    public static class Builder {
        private String message;
        private SortedMap<String, Entry> entries;

        public Builder() {
            message = null;
            entries = new TreeMap<>();
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public void addEntry(String key, Entry entry) {
            entries.put(key, entry);
        }

        public Manifest build() {
            return new Manifest(message, entries);
        }
    }

    private String message;
    private Map<String, Entry> entries;

    private Manifest(String message, Map<String, Entry> entries) {
        this.message = message;
        this.entries = entries;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Manifest createFromFile(PhysicalKey path) throws IOException, URISyntaxException {
        Builder builder = builder();

        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(path.getInputStream()))) {
            String header = reader.readLine();
            JsonNode node = mapper.readTree(header);
            String version = node.get("version").asText();
            if (!version.equals(VERSION)) {
                throw new IOException("Unsupported manifest version: " + version);
            }
            String message = node.get("message").asText();
            builder.setMessage(message);
            // TODO: package metadata

            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode row = mapper.readTree(line);

                String logicalKey = row.get("logical_key").asText();
                String physicalKeyString = row.get("physical_keys").get(0).asText();
                PhysicalKey physicalKey = PhysicalKey.fromUri(new URI(physicalKeyString));
                long size = row.get("size").asLong();
                JsonNode hashNode = row.get("hash");
                Entry.HashType hashType = Entry.HashType.valueOf(hashNode.get("type").asText());
                String hashValue = hashNode.get("value").asText();
                // TODO: entry metadata

                Entry entry = new Entry(physicalKey, size, new Entry.Hash(hashType, hashValue));
                builder.addEntry(logicalKey, entry);
            }
        }

        return builder.build();
    }

    public void serializeToOutputStream(OutputStream out) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode header = mapper.createObjectNode()
            .put("version", VERSION)
            .put("message", message);
            // TODO: meta

        out.write(mapper.writeValueAsBytes(header));
        out.write('\n');

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
            row.set("meta", mapper.createObjectNode()); // TODO

            out.write(mapper.writeValueAsBytes(row));
            out.write('\n');
        }
    }

    public String calculateTopHash() throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

        // NOTE: Keys must be ordered alphabetically!
        // ORDER_MAP_ENTRIES_BY_KEYS only applies to whole objects, not to keys set explicitly.
        ObjectNode header = mapper.createObjectNode()
            .put("message", message)
            .put("version", VERSION);
            // TODO: meta

        digest.update(mapper.writeValueAsBytes(header));

        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            String logicalKey = e.getKey();
            Entry entry = e.getValue();

            if (entry.getHash() == null) {
                throw new IOException("Cannot calculate top hash for entries without hashes!");
            }

            // Use a map to take advantage of key sorting.
            Map<String, Object> row = Map.of(
                "logical_key", logicalKey,
                "hash", entry.getHash(),
                "size", entry.getSize(),
                "meta", Map.of()
            );

            byte[] bytes = mapper.writeValueAsBytes(row);
            String tmp = new String(bytes);
            System.out.println(tmp);
            digest.update(bytes);
        }

        return BinaryUtils.toHex(digest.digest());
    }

    public String getMessage() {
        return message;
    }


    public Entry getEntry(String logicalKey) {
        return entries.get(logicalKey);
    }

    private Path resolveDest(Path dest, String logicalKey) throws IOException {
        Path entryDest = dest.resolve(logicalKey).normalize();
        if (!entryDest.startsWith(dest)) {
            throw new IOException("Invalid logical key: " + logicalKey);
        }
        Files.createDirectories(entryDest.getParent());
        return entryDest;
    }

    public void install(Path dest) throws IOException {
        // TODO: save the manifest to the local registry?

        Map<String, List<Map.Entry<String, Entry>>> entriesByBucket =
            entries.entrySet().stream().collect(Collectors.groupingBy(
                entry -> entry.getValue().getPhysicalKey() instanceof S3PhysicalKey
                    ? ((S3PhysicalKey)entry.getValue().getPhysicalKey()).getBucket()
                    : ""
            ));

        // Ideally, we would parallelize all downloads, but S3TransferManager is per-region.
        // But, a single-bucket manifest is the common case, so group all paths by buckets,
        // then parallelize everything within the bucket.
        for (Map.Entry<String, List<Map.Entry<String, Entry>>> e : entriesByBucket.entrySet()) {
            String bucket = e.getKey();
            List<Map.Entry<String, Entry>> bucketEntries = e.getValue();

            if (bucket.length() == 0) {
                // Local files
                throw new IOException("Expected s3 paths, but got local paths");
            } else {
                S3AsyncClient s3 = S3ClientStore.getAsyncClient(bucket);

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
                }
            }
        }
    }

    public Manifest push(Namespace namespace, String message) throws IOException, NoSuchAlgorithmException {
        PhysicalKey namespacePath = namespace.getPath();
        if (!(namespacePath instanceof S3PhysicalKey)) {
            throw new IOException("Only S3 namespace supported");
        }

        S3PhysicalKey s3NamespacePath = (S3PhysicalKey)namespacePath;
        String destBucket = s3NamespacePath.getBucket();
        S3AsyncClient s3 = S3ClientStore.getAsyncClient(destBucket);

        Map<String, Entry> entriesWithHashes = entries.entrySet()
            .stream()
            .parallel()
            .map(entry -> {
                try {
                    return Map.entry(entry.getKey(), entry.getValue().withHash());
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Builder builder = builder();
        builder.setMessage(message);

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

                UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                    .putObjectRequest(req -> req.bucket(destBucket).key(destPath))
                    .addTransferListener(LoggingTransferListener.create())
                    .source(Path.of(localSrc.getPath()))
                    .build();

                FileUpload uploadFile = transferManager.uploadFile(uploadFileRequest);
                futures.add(Map.entry(logicalKey, uploadFile.completionFuture()));
            }

            for (Map.Entry<String, CompletableFuture<CompletedFileUpload>> future : futures) {
                PutObjectResponse uploadResponse = future.getValue().join().response();

                String logicalKey = future.getKey();
                Entry origEntry = entriesWithHashes.get(logicalKey);
                String destPath = namespace.getName() + "/" + logicalKey;
                S3PhysicalKey dest = new S3PhysicalKey(destBucket, destPath, uploadResponse.versionId());
                builder.addEntry(logicalKey, new Entry(dest, origEntry.getSize(), origEntry.getHash()));
            }
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
