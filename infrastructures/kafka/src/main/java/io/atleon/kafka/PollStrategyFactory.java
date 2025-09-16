package io.atleon.kafka;

/**
 * Factory that provides instances of {@link PollStrategy} upon beginning the consumption of
 * records from Kafka.
 */
public interface PollStrategyFactory {

    /**
     * Creates a factory that always returns a "natural" polling strategy.
     *
     * @see PollStrategy#natural()
     */
    static PollStrategyFactory natural() {
        return PollStrategy::natural;
    }

    /**
     * Creates a factory that always returns a "binary strides" polling strategy.
     *
     * @see PollStrategy#binaryStrides()
     */
    static PollStrategyFactory binaryStrides() {
        return PollStrategy::binaryStrides;
    }

    /**
     * Creates a factory that always returns a "greatest batch lag" polling strategy.
     *
     * @see PollStrategy#greatestBatchLag()
     */
    static PollStrategyFactory greatestBatchLag() {
        return PollStrategy::greatestBatchLag;
    }

    PollStrategy create();
}
