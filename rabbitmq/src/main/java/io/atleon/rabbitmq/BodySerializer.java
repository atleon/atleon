package io.atleon.rabbitmq;

public interface BodySerializer<T> extends Configurable {

    SerializedBody serialize(T t);
}
