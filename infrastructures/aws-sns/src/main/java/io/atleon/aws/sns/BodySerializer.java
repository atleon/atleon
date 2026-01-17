package io.atleon.aws.sns;

import io.atleon.util.Configurable;

import java.util.Map;

/**
 * An interface for converting objects to SNS Message bodies.
 *
 * @param <T> Type to be serialized from
 */
public interface BodySerializer<T> extends Configurable {

    @Override
    default void configure(Map<String, ?> properties) {}

    String serialize(T data);
}
