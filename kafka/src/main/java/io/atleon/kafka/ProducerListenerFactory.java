package io.atleon.kafka;

/**
 * Factory that provides instances of {@link ProducerListener} upon beginning the production of
 * records to Kafka.
 */
public interface ProducerListenerFactory {

    /**
     * Creates a factory that always returns a no-op listener.
     */
    static ProducerListenerFactory noOp() {
        return singleton(ProducerListener.noOp());
    }

    /**
     * Creates a factory that will always return the provided listener instance for each production
     * process.
     */
    static ProducerListenerFactory singleton(ProducerListener listener) {
        return __ -> listener;
    }

    /**
     * Create a new listener with access to the active consumer instance via
     * {@link ProducerInvocable} API.
     */
    ProducerListener create(ProducerInvocable invocable);
}
