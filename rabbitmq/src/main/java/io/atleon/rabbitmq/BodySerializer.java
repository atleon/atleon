package io.atleon.rabbitmq;

public interface BodySerializer<T> extends Configurable {

    byte[] serialize(T t);
}
