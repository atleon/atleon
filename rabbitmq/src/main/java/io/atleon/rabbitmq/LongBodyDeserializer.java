package io.atleon.rabbitmq;

import java.nio.ByteBuffer;
import java.util.Map;

public class LongBodyDeserializer implements BodyDeserializer<Long> {

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Long deserialize(SerializedBody body) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(body.bytes());
        buffer.flip();
        return buffer.getLong();
    }
}
