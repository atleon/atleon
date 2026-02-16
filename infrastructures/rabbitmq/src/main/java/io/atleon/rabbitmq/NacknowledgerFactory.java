package io.atleon.rabbitmq;

import io.atleon.util.Configurable;
import org.slf4j.Logger;

import java.util.Map;
import java.util.function.Consumer;

/**
 * An interface for creating a "nacknowledger" ({@link Consumer} of Throwable) that is executed
 * if/when processing of the associated {@link RabbitMQMessage} is exceptionally terminated.
 *
 * @param <T> The deserialized type of received Message bodies
 */
public interface NacknowledgerFactory<T> extends Configurable {

    @Override
    default void configure(Map<String, ?> properties) {}

    Consumer<Throwable> create(ReceivedRabbitMQMessage<T> message, Nackable nackable, Consumer<Throwable> errorEmitter);

    final class Emit<T> implements NacknowledgerFactory<T> {

        Emit() {}

        @Override
        public Consumer<Throwable> create(
                ReceivedRabbitMQMessage<T> message, Nackable nackable, Consumer<Throwable> errorEmitter) {
            return errorEmitter;
        }
    }

    final class Nack<T> implements NacknowledgerFactory<T> {

        private final Logger logger;

        private final boolean requeue;

        Nack(Logger logger, boolean requeue) {
            this.logger = logger;
            this.requeue = requeue;
        }

        @Override
        public Consumer<Throwable> create(
                ReceivedRabbitMQMessage<T> message, Nackable nackable, Consumer<Throwable> errorEmitter) {
            return error -> {
                logger.warn("Nacknowledging RabbitMQ message", error);
                nackable.nack(requeue);
            };
        }
    }
}
