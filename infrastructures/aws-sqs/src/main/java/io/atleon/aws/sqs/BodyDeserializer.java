package io.atleon.aws.sqs;

import io.atleon.util.Configurable;

import java.util.Map;

/**
 * An interface for converting String SQS Message bodies to objects.
 *
 * @param <T> Type to be deserialized into
 */
public interface BodyDeserializer<T> extends Configurable {

    @Override
    default void configure(Map<String, ?> properties) {}

    T deserialize(String data);
}
