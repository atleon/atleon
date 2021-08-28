package io.atleon.rabbitmq;

import java.util.Map;

public class ByteArrayBodyDeserializer implements BodyDeserializer<byte[]> {

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public byte[] deserialize(byte[] data) {
        return data;
    }
}
