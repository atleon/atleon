package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Characterizes "strategy" for how offset state is managed during the course of assignment and
 * processing from any given {@link TopicPartition}.
 */
public interface OffsetTrackingStrategy {

    /**
     * Strategy that assigns {@link OffsetTracker#simple(TopicPartition)} to all assigned
     * partitions.
     */
    static OffsetTrackingStrategy simple() {
        return new Simple();
    }

    /**
     * Strategy that assigns {@link OffsetTracker#commitless(TopicPartition)} to all assigned
     * partitions.
     */
    static OffsetTrackingStrategy commitless() {
        return new Commitless();
    }

    /**
     * Strategy that assigns {@link OffsetTracker#acknowledgedAheadOfCommit(ConsumerPartition)}
     * to all assigned partitions.
     */
    static OffsetTrackingStrategy acknowledgedAheadOfCommit() {
        return (consumer, partitions) -> ConsumerPartition.create(consumer, partitions).stream()
                .map(OffsetTracker::acknowledgedAheadOfCommit)
                .collect(Collectors.toList());
    }

    /**
     * Creates a {@link Collection} of {@link OffsetTracker} instances for each of the
     * provided/assigned {@link TopicPartition}s. Implementations should return an entry for each
     * given partition.
     */
    Collection<OffsetTracker> assignTrackers(Consumer<?, ?> consumer, Collection<TopicPartition> partitions);

    final class Simple implements OffsetTrackingStrategy {

        private Simple() {}

        @Override
        public List<OffsetTracker> assignTrackers(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            return partitions.stream().map(OffsetTracker::simple).collect(Collectors.toList());
        }
    }

    final class Commitless implements OffsetTrackingStrategy {

        private Commitless() {}

        @Override
        public List<OffsetTracker> assignTrackers(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            return partitions.stream().map(OffsetTracker::commitless).collect(Collectors.toList());
        }
    }
}
