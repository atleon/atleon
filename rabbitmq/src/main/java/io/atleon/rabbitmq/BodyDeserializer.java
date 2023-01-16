package io.atleon.rabbitmq;

import io.atleon.util.Configurable;

public interface BodyDeserializer<T> extends Configurable {

    T deserialize(SerializedBody body);
}
