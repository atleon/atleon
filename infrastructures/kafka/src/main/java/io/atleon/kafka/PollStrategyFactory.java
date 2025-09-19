package io.atleon.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Comparator;

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

    /**
     * Creates a factory that always returns a "priority cutoff on lag" polling strategy.
     *
     * @see PollStrategy#priorityCutoffOnLag()
     */
    static PollStrategyFactory priorityCutoffOnLog() {
        return PollStrategy::priorityCutoffOnLag;
    }

    /**
     * Creates a factory that always returns a "priority cutoff on lag" polling strategy.
     *
     * @see PollStrategy#priorityCutoffOnLag(Comparator, int)
     */
    static PollStrategyFactory priorityCutoffOnLag(Comparator<TopicPartition> comparator, int threshold) {
        return () -> PollStrategy.priorityCutoffOnLag(comparator, threshold);
    }

    PollStrategy create();
}
