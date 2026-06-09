package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Objects;
import java.util.Optional;

/**
 * Hydrated descriptor of an offset associated with a consumed (or consumable)
 * {@link org.apache.kafka.clients.consumer.ConsumerRecord}. This includes the originating
 * {@link TopicPartition}, record offset, and epoch of the partition leader.
 */
final class ConsumerOffset {

    private final TopicPartition topicPartition;

    private final long offset;

    private final Optional<Integer> leaderEpoch;

    public ConsumerOffset(TopicPartition topicPartition, long offset) {
        this(topicPartition, offset, Optional.empty());
    }

    private ConsumerOffset(TopicPartition topicPartition, long offset, Optional<Integer> leaderEpoch) {
        this.topicPartition = topicPartition;
        this.offset = offset;
        this.leaderEpoch = leaderEpoch;
    }

    static ConsumerOffset create(TopicPartition topicPartition, ConsumerRecord<?, ?> consumerRecord) {
        return new ConsumerOffset(topicPartition, consumerRecord.offset(), consumerRecord.leaderEpoch());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumerOffset that = (ConsumerOffset) o;
        return offset == that.offset
                && Objects.equals(topicPartition, that.topicPartition)
                && Objects.equals(leaderEpoch, that.leaderEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, offset, leaderEpoch);
    }

    @Override
    public String toString() {
        return "ConsumerOffset{topicPartition=" + topicPartition + ", offset="
                + offset + ", leaderEpoch="
                + leaderEpoch + '}';
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public long offset() {
        return offset;
    }

    public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }
}
