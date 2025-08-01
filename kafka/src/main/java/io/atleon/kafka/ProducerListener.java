package io.atleon.kafka;

import org.apache.kafka.clients.producer.Producer;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * A listener interface for callbacks associated with the lifecycle of Kafka record production.
 * Instances are created from {@link ProducerListenerFactory#create(ProducerInvocable)}, which
 * allows for implementations to be constructed with an on-demand handle to the underlying
 * {@link Producer} instance.
 */
public interface ProducerListener {

    /**
     * Creates a listener that does nothing.
     */
    static ProducerListener noOp() {
        return new ProducerListener() {};
    }

    /**
     * Creates a listener that allows subscribing to closure event.
     */
    static Closure closure() {
        return new Closure();
    }

    /**
     * Callback invoked when the provided producer is about to be closed.
     */
    default void onClose(Producer<?, ?> producer) {

    }

    /**
     * Callback invoked after the associated producer has been closed.
     */
    default void close() {

    }

    final class Closure implements ProducerListener {

        private final Sinks.Empty<Void> closed = Sinks.empty();

        private Closure() {

        }

        @Override
        public void close() {
            closed.tryEmitEmpty();
        }

        public Mono<Void> closed() {
            return closed.asMono();
        }
    }
}
