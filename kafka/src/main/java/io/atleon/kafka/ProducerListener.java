package io.atleon.kafka;

import org.apache.kafka.clients.producer.Producer;

/**
 * A listener interface for callbacks associated with the lifecycle of Kafka record production.
 * Instances are created from {@link ProducerListenerFactory#create(ProducerInvocable)}, which
 * allows for implementations to be constructed with an on-demand handle for the underlying
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
     * Callback invoked when the provided producer is about to be closed.
     */
    default void onClose(Producer<?, ?> producer) {

    }
}
