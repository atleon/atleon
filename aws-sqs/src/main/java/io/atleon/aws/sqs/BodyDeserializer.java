package io.atleon.aws.sqs;

import io.atleon.util.Configurable;

/**
 * An interface for converting String SQS Message bodies to objects.
 *
 * A class that implements this interface is expected to have a constructor with no parameters.
 *
 * @param <T> Type to be deserialized into
 */
public interface BodyDeserializer<T> extends Configurable {

    T deserialize(String body);
}
