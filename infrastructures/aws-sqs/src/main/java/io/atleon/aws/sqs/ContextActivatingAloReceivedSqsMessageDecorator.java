package io.atleon.aws.sqs;

import io.atleon.context.ContextActivatingAloDecorator;
import io.atleon.core.Alo;

/**
 * An {@link AloReceivedSqsMessageDecorator} that decorates {@link Alo} elements with
 * {@link io.atleon.context.AloContext AloContext} activation
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedSqsMessage}s
 */
public class ContextActivatingAloReceivedSqsMessageDecorator<T>
        extends ContextActivatingAloDecorator<ReceivedSqsMessage<T>> implements AloReceivedSqsMessageDecorator<T> {}
