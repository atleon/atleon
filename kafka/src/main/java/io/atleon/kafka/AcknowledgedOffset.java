package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * An offset from a particular {@link TopicPartition} that has been acknowledged, and contains the
 * {@link OffsetAndMetadata} to commit, which is the offset <i>just after</i> the record for which
 * this acknowledgement is associated.
 */
final class AcknowledgedOffset {

    private final TopicPartition topicPartition;

    private final OffsetAndMetadata nextOffsetAndMetadata;

    public AcknowledgedOffset(TopicPartition topicPartition, OffsetAndMetadata nextOffsetAndMetadata) {
        this.topicPartition = topicPartition;
        this.nextOffsetAndMetadata = nextOffsetAndMetadata;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public OffsetAndMetadata nextOffsetAndMetadata() {
        return nextOffsetAndMetadata;
    }
}
