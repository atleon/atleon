package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

final class CommittableOffset {

    private final TopicPartition partition;

    private final OffsetAndMetadata offset;

    public CommittableOffset(TopicPartition partition, OffsetAndMetadata offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public TopicPartition partition() {
        return partition;
    }

    public OffsetAndMetadata offset() {
        return offset;
    }
}
