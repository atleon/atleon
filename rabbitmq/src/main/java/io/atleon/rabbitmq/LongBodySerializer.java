package io.atleon.rabbitmq;

import java.nio.ByteBuffer;
import java.util.Map;

public class LongBodySerializer implements BodySerializer<Long> {

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public SerializedBody serialize(Long aLong) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(aLong);
        return SerializedBody.ofBytes(buffer.array());
    }
}
