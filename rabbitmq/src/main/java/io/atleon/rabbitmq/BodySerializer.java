package io.atleon.rabbitmq;

import io.atleon.util.Configurable;

import java.util.Map;

public interface BodySerializer<T> extends Configurable {

    @Override
    default void configure(Map<String, ?> properties) {

    }

    SerializedBody serialize(T t);
}
