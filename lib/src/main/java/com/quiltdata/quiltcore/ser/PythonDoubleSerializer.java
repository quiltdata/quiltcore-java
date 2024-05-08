package com.quiltdata.quiltcore.ser;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Serializes a Double value into a JSON string using Python formatting.
 */
public class PythonDoubleSerializer extends JsonSerializer<Double> {
    /**
     * Serializes a Double value into JSON format.
     *
     * @param value       The Double value to be serialized.
     * @param gen         The JsonGenerator object used for writing JSON content.
     * @param serializers The SerializerProvider object used for accessing serializers.
     * @throws IOException If an I/O error occurs while writing JSON content.
     */
    @Override
    public void serialize(Double value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeRawValue(PythonFormat.formatDouble(value));
    }
}
