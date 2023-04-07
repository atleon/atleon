package io.atleon.rabbitmq;

import java.nio.charset.StandardCharsets;

public final class StringBodyDeserializer implements BodyDeserializer<String> {

    @Override
    public String deserialize(SerializedBody body) {
        return new String(body.bytes(), StandardCharsets.UTF_8);
    }
}
