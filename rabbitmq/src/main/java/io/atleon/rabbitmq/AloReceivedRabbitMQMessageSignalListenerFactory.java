package io.atleon.rabbitmq;

import io.atleon.core.AloSignalListenerFactory;

/**
 * Interface through which side effects on reactive pipeline signals emitted from
 * Reactor Publishers of {@link io.atleon.core.Alo}s referencing {@link ReceivedRabbitMQMessage}s
 * can be implemented.
 * <p>
 * In order to have implementations automatically applied, you can use the
 * {@link java.util.ServiceLoader} SPI and add the class names to
 * {@code META-INF/services/io.atleon.rabbitmq.AloReceivedRabbitMQMessageSignalListenerFactory} in
 * your project's resource directory.
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedRabbitMQMessage}s
 */
public interface AloReceivedRabbitMQMessageSignalListenerFactory<T, STATE>
    extends AloSignalListenerFactory<ReceivedRabbitMQMessage<T>, STATE> {

    /**
     * This parameter will be populated during configuration to let the factory know the name of
     * the queue that is being consumed from.
     */
    String QUEUE_CONFIG = "alo.signal.listener.factory.rabbitmq.queue";
}
