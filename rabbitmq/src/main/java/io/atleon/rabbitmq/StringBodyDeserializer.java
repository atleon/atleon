package io.atleon.rabbitmq;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class StringBodyDeserializer implements BodyDeserializer<String> {

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public String deserialize(byte[] data) {
        return new String(data, StandardCharsets.UTF_8);
    }
}
