package io.atleon.rabbitmq;

import java.nio.ByteBuffer;

public final class LongBodySerializer implements BodySerializer<Long> {

    @Override
    public SerializedBody serialize(Long data) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(data);
        return SerializedBody.ofBytes(buffer.array());
    }
}
