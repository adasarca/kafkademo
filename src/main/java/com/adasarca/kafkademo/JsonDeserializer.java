package com.adasarca.kafkademo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class JsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> type;

    public JsonDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            return this.objectMapper.readValue(bytes, type);
        } catch (IOException e) {
            throw new SerializationException(String.format("Error deserializing JSON object [%s]: ", new String(bytes)), e);
        }
    }
}
