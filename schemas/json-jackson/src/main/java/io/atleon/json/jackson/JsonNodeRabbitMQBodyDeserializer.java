package io.atleon.json.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import io.atleon.rabbitmq.BodyDeserializer;
import io.atleon.rabbitmq.SerializedBody;

public final class JsonNodeRabbitMQBodyDeserializer implements BodyDeserializer<JsonNode> {

    private final ObjectMapperFacade objectMapperFacade = ObjectMapperFacade.create();

    @Override
    public JsonNode deserialize(SerializedBody data) {
        return objectMapperFacade.readAsNode(data.bytes());
    }
}
