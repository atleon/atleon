package io.atleon.rabbitmq;

import io.atleon.util.Configurable;

import java.util.Map;

public interface BodyDeserializer<T> extends Configurable {

    @Override
    default void configure(Map<String, ?> properties) {

    }

    T deserialize(SerializedBody body);
}
