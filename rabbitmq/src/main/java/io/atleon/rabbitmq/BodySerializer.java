package io.atleon.rabbitmq;

import io.atleon.util.Configurable;

public interface BodySerializer<T> extends Configurable {

    SerializedBody serialize(T t);
}
