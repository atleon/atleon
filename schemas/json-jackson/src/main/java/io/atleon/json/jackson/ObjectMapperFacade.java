package io.atleon.json.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

final class ObjectMapperFacade {

    private final ObjectMapper objectMapper;

    private ObjectMapperFacade(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public static ObjectMapperFacade create() {
        return new ObjectMapperFacade(new ObjectMapper());
    }

    public static ObjectMapperFacade wrap(ObjectMapper objectMapper) {
        return new ObjectMapperFacade(objectMapper);
    }

    public byte[] writeAsBytes(Object data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to write data.", e);
        }
    }

    public String writeAsString(Object data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to write data.", e);
        }
    }

    public <T> T readAs(byte[] data, Class<? extends T> type) {
        try {
            return objectMapper.readValue(data, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read JSON data.", e);
        }
    }

    public <T> T readAs(String data, Class<? extends T> type) {
        try {
            return objectMapper.readValue(data, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read JSON data.", e);
        }
    }

    public JsonNode readAsNode(byte[] data) {
        try {
            return objectMapper.readTree(data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read JSON data.", e);
        }
    }

    public JsonNode readAsNode(String data) {
        try {
            return objectMapper.readTree(data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read JSON data.", e);
        }
    }
}
