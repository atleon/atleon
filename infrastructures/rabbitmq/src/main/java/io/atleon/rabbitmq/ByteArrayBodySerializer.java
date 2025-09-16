package io.atleon.rabbitmq;

public final class ByteArrayBodySerializer implements BodySerializer<byte[]> {

    @Override
    public SerializedBody serialize(byte[] data) {
        return SerializedBody.ofBytes(data);
    }
}
