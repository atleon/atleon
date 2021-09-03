package io.atleon.rabbitmq;

public interface BodyDeserializer<T> extends Configurable {

    T deserialize(SerializedBody body);
}
