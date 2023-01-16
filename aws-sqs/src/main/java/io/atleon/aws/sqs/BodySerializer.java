package io.atleon.aws.sqs;

import io.atleon.util.Configurable;

/**
 * An interface for converting objects to SQS Message bodies.
 *
 * A class that implements this interface is expected to have a constructor with no parameter.
 *
 * @param <T> Type to be serialized from
 */
public interface BodySerializer<T> extends Configurable {

    String serialize(T body);
}
