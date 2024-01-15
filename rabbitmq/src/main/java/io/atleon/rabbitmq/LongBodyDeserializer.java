package io.atleon.rabbitmq;

import java.nio.Buffer;
import java.nio.ByteBuffer;

public final class LongBodyDeserializer implements BodyDeserializer<Long> {

    @Override
    public Long deserialize(SerializedBody data) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(data.bytes());
        Buffer.class.cast(buffer).flip(); // Cast added for forward compatibility
        return buffer.getLong();
    }
}
