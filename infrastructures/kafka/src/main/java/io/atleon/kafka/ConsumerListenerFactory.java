package io.atleon.kafka;

/**
 * Factory that provides instances of {@link ConsumerListener} upon beginning the consumption of
 * records from Kafka.
 */
public interface ConsumerListenerFactory {

    /**
     * Creates a factory that always returns a no-op listener.
     */
    static ConsumerListenerFactory noOp() {
        return singleton(ConsumerListener.noOp());
    }

    /**
     * Creates a factory that will always return the provided listener instance for each
     * consumption process.
     */
    static ConsumerListenerFactory singleton(ConsumerListener listener) {
        return __ -> listener;
    }

    /**
     * Create a new listener with access to the active consumer instance via
     * {@link ConsumerInvocable} API.
     */
    ConsumerListener create(ConsumerInvocable invocable);
}
