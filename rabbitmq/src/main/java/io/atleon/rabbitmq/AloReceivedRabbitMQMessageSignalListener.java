package io.atleon.rabbitmq;

import io.atleon.core.AloSignalListener;

/**
 * Interface through which side effects on {@link reactor.core.publisher.Signal}s emitted from
 * Reactor Publishers of {@link io.atleon.core.Alo}s referencing {@link ReceivedRabbitMQMessage}s
 * can be implemented.
 * <p>
 * In order to have implementations automatically applied, you can use the
 * {@link java.util.ServiceLoader} SPI and add the class names to
 * {@code META-INF/services/io.atleon.rabbitmq.AloReceivedRabbitMQMessageSignalListener} in your
 * project's resource directory.
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedRabbitMQMessage}s
 */
public interface AloReceivedRabbitMQMessageSignalListener<T> extends AloSignalListener<ReceivedRabbitMQMessage<T>> {

    /**
     * This parameter will be populated during configuration to let the decorator know the name of
     * the queue that is being consumed from.
     */
    String QUEUE_CONFIG = "alo.signal.listener.rabbitmq.queue";
}
