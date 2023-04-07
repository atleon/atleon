package io.atleon.aws.sns;

/**
 * An SNS {@link BodySerializer} that serializes objects using their {@link Object#toString()}.
 *
 * @param <T> Type to be serialized from
 */
public final class StringBodySerializer<T> implements BodySerializer<T> {

    @Override
    public String serialize(T body) {
        return body.toString();
    }
}
