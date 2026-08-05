package io.atleon.kafka;

import java.util.Map;

/**
 * Factory that provides instances of {@link ReceptionListener} upon beginning the reception of
 * records from Kafka.
 */
public interface ReceptionListenerFactory {

    /**
     * Creates a factory that always returns a no-op listener.
     */
    static ReceptionListenerFactory noOp() {
        return singleton(ReceptionListener.noOp());
    }

    /**
     * Creates a factory that will always return the provided listener instance for each
     * consumption process.
     */
    static ReceptionListenerFactory singleton(ReceptionListener receptionListener) {
        return () -> receptionListener;
    }

    /**
     * Invoked at subscription time to allow this factory to incorporate the provided consumer
     * properties in the configuration of subsequently-created {@link ReceptionListener} instances.
     */
    default ReceptionListenerFactory withConsumerProperties(Map<String, ?> consumerProperties) {
        return this;
    }

    ReceptionListener create();
}
