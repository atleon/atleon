package io.atleon.rabbitmq;

import java.nio.ByteBuffer;

public final class LongBodyDeserializer implements BodyDeserializer<Long> {

    @Override
    public Long deserialize(SerializedBody body) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(body.bytes());
        buffer.flip();
        return buffer.getLong();
    }
}
