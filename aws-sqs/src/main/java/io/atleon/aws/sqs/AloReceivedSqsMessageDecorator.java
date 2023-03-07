package io.atleon.aws.sqs;

import io.atleon.core.AloDecorator;

/**
 * Interface through which decoration of {@link io.atleon.core.Alo}s referencing
 * {@link ReceivedSqsMessage}s can be implemented.
 * <p>
 * In order to have implementations automatically applied, you can use the
 * {@link java.util.ServiceLoader} SPI and add the class names to
 * {@code META-INF/services/io.atleon.aws.sqs.AloReceivedSqsMessageDecorator} in your project's
 * resource directory.
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedSqsMessage}s
 */
public interface AloReceivedSqsMessageDecorator<T> extends AloDecorator<ReceivedSqsMessage<T>> {

}
