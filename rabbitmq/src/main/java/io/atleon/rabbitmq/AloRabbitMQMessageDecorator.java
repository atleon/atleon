package io.atleon.rabbitmq;

import io.atleon.core.AloDecorator;

/**
 * Interface through which decoration of {@link io.atleon.core.Alo}s referencing
 * {@link RabbitMQMessage}s can be implemented.
 * <p>
 * In order to have implementations automatically applied, you can use the
 * {@link java.util.ServiceLoader} SPI and add the class names to
 * {@code META-INF/services/io.atleon.rabbitmq.AloRabbitMQMessageDecorator} in your project's
 * resource directory.
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link RabbitMQMessage}s
 */
public interface AloRabbitMQMessageDecorator<T> extends AloDecorator<RabbitMQMessage<T>> {

}