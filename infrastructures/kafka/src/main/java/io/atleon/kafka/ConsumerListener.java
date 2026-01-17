package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * A listener interface for callbacks associated with the lifecycle of Kafka record consumption.
 * Instances are created from {@link ConsumerListenerFactory#create(ConsumerInvocable)}, which
 * allows for implementations to be constructed with an on-demand handle to the underlying
 * {@link Consumer} instance.
 */
public interface ConsumerListener {

    /**
     * Creates a listener that does nothing.
     */
    static ConsumerListener noOp() {
        return new ConsumerListener() {};
    }

    /**
     * Creates a listener that will seek to the specified offset on the provided partition the
     * first time this partition is assigned.
     */
    static ConsumerListener seekOnce(TopicPartition topicPartition, long offset) {
        return seekOnce(Collections.singletonMap(topicPartition, offset));
    }

    /**
     * Creates a listener that will seek to the specified offsets on each mapped partition the
     * first time such a provided partition is assigned.
     */
    static ConsumerListener seekOnce(Map<TopicPartition, Long> offsets) {
        return doOnPartitionsAssignedOnce((consumer, partitions) -> {
            for (TopicPartition partition : partitions) {
                Long offset = offsets.get(partition);
                if (offset != null) {
                    consumer.seek(partition, offset);
                }
            }
        });
    }

    /**
     * Creates a listener that will seek to the beginning offset of any given partition the first
     * time it is assigned.
     */
    static ConsumerListener seekToBeginningOnce() {
        return doOnPartitionsAssignedOnce(Consumer::seekToBeginning);
    }

    /**
     * Creates a listener that will perform some action on any given partition the first time it is
     * assigned.
     */
    static ConsumerListener doOnPartitionsAssignedOnce(BiConsumer<Consumer<?, ?>, Collection<TopicPartition>> action) {
        Set<TopicPartition> actioned = new HashSet<>();
        return new ConsumerListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                Collection<TopicPartition> actionable =
                        partitions.stream().filter(it -> !actioned.contains(it)).collect(Collectors.toList());
                if (!actionable.isEmpty()) {
                    action.accept(consumer, actionable);
                    actioned.addAll(actionable);
                }
            }
        };
    }

    /**
     * Creates a listener that allows subscribing to closure event.
     */
    static Closure closure() {
        return new Closure();
    }

    /**
     * Callback invoked when it has been detected that the provided partitions have been lost.
     *
     * @param consumer   Consumer handle on which it is safe to invoke blocking calls
     * @param partitions The partitions that have been lost
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsLost(Collection)
     */
    default void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        onPartitionsRevoked(consumer, partitions);
    }

    /**
     * Callback invoked when the provided partitions have been revoked.
     *
     * @param consumer   Consumer handle on which it is safe to invoke blocking calls
     * @param partitions The partitions that have been revoked
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(Collection)
     */
    default void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}

    /**
     * Callback invoked when the provided partitions have been assigned.
     *
     * @param consumer   Consumer handle on which it is safe to invoke blocking calls
     * @param partitions The partitions that have been assigned
     * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(Collection)
     */
    default void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}

    /**
     * Callback invoked when the provided consumer is about to be closed.
     */
    default void onClose(Consumer<?, ?> consumer) {}

    /**
     * Callback invoked after the associated consumer has been closed.
     */
    default void close() {}

    final class Closure implements ConsumerListener {

        private final Sinks.Empty<Void> closed = Sinks.empty();

        private Closure() {}

        @Override
        public void close() {
            closed.tryEmitEmpty();
        }

        public Mono<Void> closed() {
            return closed.asMono();
        }
    }
}
