package io.atleon.rabbitmq;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class StringBodyDeserializer implements BodyDeserializer<String> {

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public String deserialize(SerializedBody body) {
        return new String(body.bytes(), StandardCharsets.UTF_8);
    }
}
