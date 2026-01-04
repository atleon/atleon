package io.atleon.aws.sns;

import java.util.function.Function;

/**
 * Implementation of process to create an {@link SnsMessage} from a Message body payload
 *
 * @param <T> The (deserialized) type of body referenced by the resulting {@link SnsMessage}
 */
@FunctionalInterface
public interface SnsMessageCreator<T> extends Function<T, SnsMessage<T>> {}
