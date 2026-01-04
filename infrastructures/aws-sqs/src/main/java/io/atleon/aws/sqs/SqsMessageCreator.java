package io.atleon.aws.sqs;

import java.util.function.Function;

/**
 * Implementation of process to create an {@link SqsMessage} from a Message body payload
 *
 * @param <T> The (deserialized) type of body referenced by the resulting {@link SqsMessage}
 */
@FunctionalInterface
public interface SqsMessageCreator<T> extends Function<T, SqsMessage<T>> {}
