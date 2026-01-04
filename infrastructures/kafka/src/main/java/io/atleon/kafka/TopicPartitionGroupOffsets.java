package io.atleon.kafka;

import org.apache.kafka.common.TopicPartition;

/**
 * Describes the relevant offsets for a {@link TopicPartition} in the context its consumption by a
 * particular consumer group. This allows for estimating the "lag" of the given consumer group by
 * comparing the latest offset on the partition to the committed offset for the consumer group.
 */
public final class TopicPartitionGroupOffsets {

    private final TopicPartition topicPartition;

    private final long topicPartitionLatestOffset;

    private final String groupId;

    private final long groupOffset;

    public TopicPartitionGroupOffsets(
            TopicPartition topicPartition, long topicPartitionLatestOffset, String groupId, long groupOffset) {
        this.topicPartition = topicPartition;
        this.topicPartitionLatestOffset = topicPartitionLatestOffset;
        this.groupId = groupId;
        this.groupOffset = groupOffset;
    }

    /**
     * Estimates the lag for the given {@link TopicPartition} and consumer group. It cannot be
     * guaranteed that this estimation is or ever was exactly correct due to the facts that the
     * queried offsets cannot be queried atomically and that it may be possible that there is no
     * record at any given offset due to compaction, or other form of deletion.
     */
    public long estimateLag() {
        return topicPartitionLatestOffset - groupOffset;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public String groupId() {
        return groupId;
    }

    public long groupOffset() {
        return groupOffset;
    }
}
