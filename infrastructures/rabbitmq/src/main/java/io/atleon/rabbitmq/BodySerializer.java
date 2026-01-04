package io.atleon.rabbitmq;

import io.atleon.util.Configurable;
import java.util.Map;

/**
 * An interface for converting objects to RabbitMQ Message bodies.
 *
 * @param <T> Type to be serialized from
 */
public interface BodySerializer<T> extends Configurable {

    @Override
    default void configure(Map<String, ?> properties) {}

    SerializedBody serialize(T data);
}
