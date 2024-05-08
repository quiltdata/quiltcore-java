package com.quiltdata.quiltcore;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
/**
 * A Jackson class that represents a JSON object node.
 * 
 * <p>
 * An `ObjectNode` is a container for key-value pairs, where the keys are strings and the values can be any valid JSON node.
 * It provides methods to manipulate and access the key-value pairs within the object.
 * </p>
 * 
 * <p>
 * This class is part of the Jackson JSON library, which provides a fast and flexible way to process JSON data in Java.
 * </p>
 */
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.quiltdata.quiltcore.key.PhysicalKey;

import software.amazon.awssdk.utils.BinaryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an entry in the Quilt data repository.
 * An entry contains information about a file or object stored in the repository.
 */
/**
 * Represents an entry in the Quilt dataset.
 */
public class Entry {
    private static final Logger logger = LoggerFactory.getLogger(Entry.class);

    /**
     * Enumerates the types of hash algorithms supported by Quilt.
     */
    public static enum HashType {
        /**
         * The SHA-256 hash algorithm.
         */
        SHA256,
    }

    /**
     * Represents a hash value.
     */
    public static class Hash {
        /**
         * The type of the hash.
         */
        public final HashType type;
        /**
         * The value of the hash.
         */
        public final String value;

        /**
         * Constructs a new Hash object with the specified type and value.
         *
         * @param type  the type of the hash
         * @param value the value of the hash
         */
        public Hash(HashType type, String value) {
            this.type = type;
            this.value = value;
        }
    }

    private final PhysicalKey physicalKey; // The physical key of the entry
    private final long size; // The size of the entry in bytes
    private final Hash hash; // The hash value of the entry
    private final ObjectNode metadata; // The metadata associated with the entry

    /**
     * Constructs a new Entry object with the specified parameters.
     *
     * @param physicalKey the physical key of the entry
     * @param size        the size of the entry in bytes
     * @param hash        the hash value of the entry
     * @param metadata    the metadata associated with the entry
     */
    public Entry(PhysicalKey physicalKey, long size, Hash hash, ObjectNode metadata) {
        this.physicalKey = physicalKey;
        this.size = size;
        this.hash = hash;
        this.metadata = metadata == null ? JsonNodeFactory.instance.objectNode() : metadata.deepCopy();
    }

    /**
     * Returns the physical key of the entry.
     *
     * @return the physical key
     */
    public PhysicalKey getPhysicalKey() {
        return physicalKey;
    }

    /**
     * Returns the size of the entry in bytes.
     *
     * @return the size of the entry
     */
    public long getSize() {
        return size;
    }

    /**
     * Returns the hash value of the entry.
     *
     * @return the hash value
     */
    public Hash getHash() {
        return hash;
    }

    /**
     * Returns the metadata associated with the entry.
     *
     * @return the metadata
     */
    public ObjectNode getMetadata() {
        return metadata.deepCopy();
    }

    /**
     * Returns the entry as a byte array.
     *
     * @return the entry as a byte array
     * @throws IOException if an I/O error occurs
     */
    public byte[] getBytes() throws IOException {
        // TODO: Verify the hash.

        return physicalKey.getBytes();
    }

    /**
     * Returns a new Entry object with the hash value calculated for the entry.
     *
     * @return a new Entry object with the hash value calculated
     * @throws IOException if an I/O error occurs
     */
    public Entry withHash() throws IOException {
        if (hash != null) {
            return this;
        }

        MessageDigest digest;
        logger.debug("Calculating hash for entry: {}", physicalKey);
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // This should never happen, and if it does happen, there's nothing we can do.
            // Let's not require the caller to handle it.
            throw new RuntimeException(e);
        }

        logger.debug("Reading entry: {}", physicalKey);
        try (InputStream in = physicalKey.getInputStream()) {
            byte[] buffer = new byte[4096];
            int count;
            while ((count = in.read(buffer)) != -1) {
                digest.update(buffer, 0, count);
            }
        }
        String hash = BinaryUtils.toHex(digest.digest());
        return new Entry(physicalKey, size, new Hash(HashType.SHA256, hash), metadata);
    }
}
