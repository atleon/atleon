package io.atleon.kafka;

/**
 * Factory that provides instances of {@link ReceptionListener} upon beginning the reception of
 * records from Kafka.
 */
public interface ReceptionListenerFactory {

    /**
     * Creates a factory that always returns a no-op listener
     */
    static ReceptionListenerFactory noOp() {
        return singleton(ReceptionListener.noOp());
    }

    /**
     * Creates a factory that will always return the provided listener instance for each reception
     * process.
     */
    static ReceptionListenerFactory singleton(ReceptionListener listener) {
        return __ -> listener;
    }

    /**
     * Create a new listener with access to the active consumer instance via
     * {@link ConsumerInvocable} API.
     */
    ReceptionListener create(ConsumerInvocable invocable);
}
