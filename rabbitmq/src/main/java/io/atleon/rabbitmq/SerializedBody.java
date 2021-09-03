package io.atleon.rabbitmq;

public interface SerializedBody {

    static SerializedBody ofBytes(byte[] bytes) {
        return () -> bytes;
    }

    byte[] bytes();
}
