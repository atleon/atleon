package io.atleon.json.jackson;

import com.fasterxml.jackson.databind.JsonNode;
import io.atleon.aws.sqs.BodyDeserializer;

public final class JsonNodeSqsBodyDeserializer implements BodyDeserializer<JsonNode> {

    private final ObjectMapperFacade objectMapperFacade = ObjectMapperFacade.create();

    @Override
    public JsonNode deserialize(String data) {
        return objectMapperFacade.readAsNode(data);
    }
}
