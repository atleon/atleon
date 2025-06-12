package io.atleon.kafka;

import org.apache.kafka.common.TopicPartition;

/**
 * An offset for a particular {@link TopicPartition} that may be committed.
 */
final class CommittableOffset {

    private final TopicPartition topicPartition;

    private final SequencedOffset sequencedOffset;

    public CommittableOffset(AcknowledgedOffset acknowledgedOffset, long sequence) {
        this.topicPartition = acknowledgedOffset.topicPartition();
        this.sequencedOffset = new SequencedOffset(acknowledgedOffset.nextOffsetAndMetadata(), sequence);
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public SequencedOffset sequencedOffset() {
        return sequencedOffset;
    }
}
