package com.quiltdata.quiltcore;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

public class EntryTest {

    @Test
    void testEnumFor() {
        // Arrange
        String name = "SHA256";
        // Act
        Entry.HashType result = Entry.HashType.enumFor(name);
        // Assert
        assertEquals(Entry.HashType.SHA256, result);
    }

    @Test
    void testEnumForChunked() {
        // Arrange
        String name = "sha2-256-chunked";
        // Act
        Entry.HashType result = Entry.HashType.enumFor(name);
        // Assert
        assertEquals(Entry.HashType.SHA2_256_Chunked, result);
    }

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
