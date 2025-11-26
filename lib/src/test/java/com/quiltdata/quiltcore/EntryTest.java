package com.quiltdata.quiltcore;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

public class EntryTest {



    @Test
    void testEnumForSha256Chunked() {
        // Arrange
        String name = "sha2-256-chunked";
        // Act
        Entry.HashType result = Entry.HashType.enumFor(name);
        // Assert
        assertEquals(Entry.HashType.SHA256_CHUNKED, result);
    }

    @Test
    void testEnumForSha256ChunkedVariants() {
        assertEquals(Entry.HashType.SHA256_CHUNKED, Entry.HashType.enumFor("sha256-chunked"));
        assertEquals(Entry.HashType.SHA256_CHUNKED, Entry.HashType.enumFor("sha256_chunked"));
    }

    @Test
    void testEnumForCRC64NVME() {
        // Arrange
        String name = "CRC64NVME";
        // Act
        Entry.HashType result = Entry.HashType.enumFor(name);
        // Assert
        assertEquals(Entry.HashType.CRC64NVME, result);
    }


    // (Old testEnumForChunked replaced by testEnumForSha256Chunked)

    @Test
    void testEnumForInvalid() {
        // Arrange
        String name = "SHA-512";
        // Act
        try {
            Entry.HashType result = Entry.HashType.enumFor(name);
            fail("Expected IllegalArgumentException");
            assert result != null;
        } catch (IllegalArgumentException e) {
            // Assert
            assertEquals("No enum constant com.quiltdata.quiltcore.Entry.HashType.SHA-512", e.getMessage());
        }
    }
    
}
