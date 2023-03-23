package io.atleon.rabbitmq;

import io.atleon.util.Configurable;
import org.slf4j.Logger;

import java.util.Map;
import java.util.function.Consumer;

public interface NacknowledgerFactory<T> extends Configurable {

    default void configure(Map<String, ?> properties) {

    }

    Consumer<Throwable> create(RabbitMQMessage<T> message, Nackable nackable, Consumer<Throwable> errorEmitter);

    final class Emit<T> implements NacknowledgerFactory<T> {

        Emit() {

        }

        @Override
        public Consumer<Throwable> create(RabbitMQMessage<T> message, Nackable nackable, Consumer<Throwable> errorEmitter) {
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
        public Consumer<Throwable> create(RabbitMQMessage<T> message, Nackable nackable, Consumer<Throwable> errorEmitter) {
            return error -> {
                logger.warn("Nacknowledging RabbitMQ message", error);
                nackable.nack(requeue);
            };
        }
    }
}
