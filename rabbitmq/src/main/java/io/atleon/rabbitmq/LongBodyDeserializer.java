package io.atleon.rabbitmq;

import java.nio.ByteBuffer;

public final class LongBodyDeserializer implements BodyDeserializer<Long> {

    @Override
    public Long deserialize(SerializedBody data) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(data.bytes());
        buffer.flip();
        return buffer.getLong();
    }
}
