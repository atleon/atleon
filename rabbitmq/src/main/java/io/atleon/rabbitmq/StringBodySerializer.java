package io.atleon.rabbitmq;

import java.nio.charset.StandardCharsets;

public final class StringBodySerializer implements BodySerializer<String> {

    @Override
    public SerializedBody serialize(String s) {
        return SerializedBody.ofBytes(s.getBytes(StandardCharsets.UTF_8));
    }
}
