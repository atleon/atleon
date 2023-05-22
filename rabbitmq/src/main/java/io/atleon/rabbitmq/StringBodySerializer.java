package io.atleon.rabbitmq;

import java.nio.charset.StandardCharsets;

public final class StringBodySerializer implements BodySerializer<String> {

    @Override
    public SerializedBody serialize(String data) {
        return SerializedBody.ofBytes(data.getBytes(StandardCharsets.UTF_8));
    }
}
