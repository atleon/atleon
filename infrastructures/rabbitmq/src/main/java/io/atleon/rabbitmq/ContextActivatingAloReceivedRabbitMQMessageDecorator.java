package io.atleon.rabbitmq;

import io.atleon.context.ContextActivatingAloDecorator;
import io.atleon.core.Alo;

/**
 * An {@link AloReceivedRabbitMQMessageDecorator} that decorates {@link Alo} elements with
 * {@link io.atleon.context.AloContext AloContext} activation
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedRabbitMQMessage}s
 */
public class ContextActivatingAloReceivedRabbitMQMessageDecorator<T>
        extends ContextActivatingAloDecorator<ReceivedRabbitMQMessage<T>>
        implements AloReceivedRabbitMQMessageDecorator<T> {}
