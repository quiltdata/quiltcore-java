package com.quiltdata.quiltcore;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quiltdata.quiltcore.key.LocalPhysicalKey;
import com.quiltdata.quiltcore.key.PhysicalKey;
import com.quiltdata.quiltcore.key.S3PhysicalKey;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;


public class Manifest {
    private Map<String, Entry> entries;

    public Manifest(Path path) throws IOException, URISyntaxException {
        entries = new HashMap<>();

        ObjectMapper mapper = new ObjectMapper();

        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            JsonNode node = mapper.readTree(header);
            String version = node.get("version").asText();
            if (!version.equals("v0")) {
                throw new IOException("Unsupported manifest version: " + version);
            }
            // TODO: package metadata

            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode row = mapper.readTree(line);

                String logicalKey = row.get("logical_key").asText();
                PhysicalKey physicalKey = parsePhysicalKey(row.get("physical_keys").get(0).asText());
                long size = row.get("size").asLong();
                JsonNode hashNode = row.get("hash");
                Entry.HashType hashType = Entry.HashType.valueOf(hashNode.get("type").asText());
                String hashValue = hashNode.get("value").asText();
                // TODO: entry metadata

                Entry entry = new Entry(physicalKey, size, new Entry.Hash(hashType, hashValue));
                entries.put(logicalKey, entry);
            }
        }
    }

    private static PhysicalKey parsePhysicalKey(String s) throws URISyntaxException {
        URI uri = new URI(s);

        if (uri.getScheme().equals("file")) {
            return new LocalPhysicalKey(uri.getPath());
        } else if (uri.getScheme().equals("s3")) {
            String bucket = uri.getHost();
            String key = uri.getPath().substring(1);  // Remove /
            String versionId = null;

            for (String nameValuePair : uri.getQuery().split("&", 1)) {
                String[] parts = nameValuePair.split("=", 1);
                if (parts.length == 2) {
                    String name = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
                    String value = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
                    if (name.equals("versionId")) {
                        versionId = value;
                    }
                }
            }

            return new S3PhysicalKey(bucket, key, versionId);
        } else {
            throw new IllegalArgumentException("Bad physical key: " + s);
        }
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
            List<Map.Entry<String, Entry>> entries = e.getValue();

            if (bucket.length() == 0) {
                // Local files
                throw new IOException("Expected s3 paths, but got local paths");
            } else {
                S3AsyncClient s3 = S3ClientStore.getClient(bucket);

                try(
                    S3TransferManager transferManager =
                        S3TransferManager.builder()
                            .s3Client(s3)
                            .build();
                ) {
                    List<CompletableFuture<CompletedFileDownload>> futures = new ArrayList<>(entries.size());

                    for (Map.Entry<String, Entry> e2 : entries) {
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
}