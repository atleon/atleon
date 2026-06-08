package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * A handle for a partition that has been assigned, and through which metadata about the assignment
 * can be accessed.
 */
public final class AssignedPartition {

    private final TopicPartition topicPartition;

    private final Supplier<Optional<OffsetAndMetadata>> committedOffsetAndMetadata;

    AssignedPartition(TopicPartition topicPartition, Supplier<Optional<OffsetAndMetadata>> committedOffsetAndMetadata) {
        this.topicPartition = topicPartition;
        this.committedOffsetAndMetadata = committedOffsetAndMetadata;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public Optional<Long> committedOffset() {
        return committedOffsetAndMetadata.get().map(OffsetAndMetadata::offset);
    }

    public Optional<String> committedMetadata() {
        return committedOffsetAndMetadata.get().map(OffsetAndMetadata::metadata);
    }
}
