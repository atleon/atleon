package io.atleon.kafka;

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

    ReceptionListener create();
}
