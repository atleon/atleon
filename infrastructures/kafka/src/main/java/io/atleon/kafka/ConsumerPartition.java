package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

/**
 * Encapsulates a {@link TopicPartition} that is either being or will imminently be consumed, with
 * extra metadata available at the time of instantiation. This is useful, for example, when the
 * given partition has been assigned and associated resource initialization needs access to the
 * current {@link Consumer#position(TopicPartition) position} and/or committed
 * {@link OffsetAndMetadata}.
 */
public final class ConsumerPartition {

    private final TopicPartition topicPartition;

    private final long position;

    private final @Nullable OffsetAndMetadata committed;

    private ConsumerPartition(TopicPartition topicPartition, long position, @Nullable OffsetAndMetadata committed) {
        this.topicPartition = topicPartition;
        this.committed = committed;
        this.position = position;
    }

    public static List<ConsumerPartition> create(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        Set<TopicPartition> partitionSet = new LinkedHashSet<>(partitions);
        Map<TopicPartition, OffsetAndMetadata> committedByPartition = consumer.committed(partitionSet);
        return partitionSet.stream()
                .map(it -> new ConsumerPartition(it, consumer.position(it), committedByPartition.get(it)))
                .collect(Collectors.toList());
    }

    public ConsumerOffset newOffsetFromPosition(LongUnaryOperator positionToOffset) {
        long offset = positionToOffset.applyAsLong(position);
        return committed != null
                ? new ConsumerOffset(topicPartition, offset, committed.leaderEpoch())
                : new ConsumerOffset(topicPartition, offset);
    }

    public Optional<String> committedMetadata() {
        return Optional.ofNullable(committed).map(OffsetAndMetadata::metadata);
    }
}
