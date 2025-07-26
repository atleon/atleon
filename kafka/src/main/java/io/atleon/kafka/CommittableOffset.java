package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * An offset for a particular {@link TopicPartition} that may be committed.
 */
final class CommittableOffset {

    private final TopicPartition topicPartition;

    private final OffsetAndMetadata offsetAndMetadata;

    private final long sequence;

    public CommittableOffset(AcknowledgedOffset acknowledgedOffset, long sequence) {
        this.topicPartition = acknowledgedOffset.topicPartition();
        this.offsetAndMetadata = acknowledgedOffset.nextOffsetAndMetadata();
        this.sequence = sequence;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public SequencedOffset sequencedOffset() {
        return new SequencedOffset(offsetAndMetadata, sequence);
    }
}
