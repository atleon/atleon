package io.atleon.aws.sns;

import java.util.Map;

/**
 * An SNS {@link BodySerializer} that serializes objects using their {@link Object#toString()}.
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
