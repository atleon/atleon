package io.atleon.json.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;

public final class JsonNodeKafkaDeserializer implements Deserializer<JsonNode> {

    private final ObjectMapperFacade objectMapperFacade = ObjectMapperFacade.create();

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        return data == null ? null : objectMapperFacade.readAsNode(data);
    }
}
