package io.atleon.rabbitmq;

import java.util.function.Function;

/**
 * Interface for creating {@link RabbitMQMessage}s from message bodies.
 *
 * @param <T> The type of message bodies
 */
public interface RabbitMQMessageCreator<T> extends Function<T, RabbitMQMessage<T>> {

    @Override
    RabbitMQMessage<T> apply(T body);
}
