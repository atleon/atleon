package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

final class CommittableOffset {

    private final TopicPartition topicPartition;

    private final SequencedOffset sequencedOffset;

    public CommittableOffset(TopicPartition topicPartition, SequencedOffset sequencedOffset) {
        this.topicPartition = topicPartition;
        this.sequencedOffset = sequencedOffset;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public OffsetAndMetadata offsetAndMetadata() {
        return sequencedOffset.offsetAndMetadata();
    }

    public SequencedOffset sequencedOffset() {
        return sequencedOffset;
    }
}
