package io.atleon.rabbitmq;

public final class ByteArrayBodyDeserializer implements BodyDeserializer<byte[]> {

    @Override
    public byte[] deserialize(SerializedBody data) {
        return data.bytes();
    }
}
