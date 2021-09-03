package io.atleon.rabbitmq;

import java.util.Map;

public class ByteArrayBodySerializer implements BodySerializer<byte[]> {

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public SerializedBody serialize(byte[] bytes) {
        return SerializedBody.ofBytes(bytes);
    }
}
