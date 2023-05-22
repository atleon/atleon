package io.atleon.rabbitmq;

import java.nio.charset.StandardCharsets;

public final class StringBodyDeserializer implements BodyDeserializer<String> {

    @Override
    public String deserialize(SerializedBody data) {
        return new String(data.bytes(), StandardCharsets.UTF_8);
    }
}
