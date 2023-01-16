package io.atleon.aws.sqs;

import java.util.Map;

/**
 * An SQS {@link BodySerializer} that serializes objects using their {@link Object#toString()}.
 *
 * @param <T> Type to be serialized from
 */
public final class StringBodySerializer<T> implements BodySerializer<T> {

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public String serialize(T body) {
        return body.toString();
    }
}
