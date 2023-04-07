package io.atleon.rabbitmq;

public final class ByteArrayBodySerializer implements BodySerializer<byte[]> {

    @Override
    public SerializedBody serialize(byte[] bytes) {
        return SerializedBody.ofBytes(bytes);
    }
}
