package io.atleon.rabbitmq;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class StringBodySerializer implements BodySerializer<String> {

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public SerializedBody serialize(String s) {
        return SerializedBody.ofBytes(s.getBytes(StandardCharsets.UTF_8));
    }
}
