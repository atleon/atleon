package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

/**
 * An offset for a {@link org.apache.kafka.clients.consumer.ConsumerRecord} from a particular
 * {@link TopicPartition} that has been acknowledged, and contains an {@link OffsetAndMetadata}
 * which may be committed, and is the offset <i>just after</i> the record for which this
 * acknowledgement is associated.
 */
final class AcknowledgedOffset {

    private final TopicPartition topicPartition;

    private final OffsetAndMetadata nextOffsetAndMetadata;

    public AcknowledgedOffset(TopicPartition topicPartition, OffsetAndMetadata nextOffsetAndMetadata) {
        this.topicPartition = topicPartition;
        this.nextOffsetAndMetadata = nextOffsetAndMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AcknowledgedOffset that = (AcknowledgedOffset) o;
        return Objects.equals(topicPartition, that.topicPartition)
                && Objects.equals(nextOffsetAndMetadata, that.nextOffsetAndMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, nextOffsetAndMetadata);
    }

    @Override
    public String toString() {
        return "AcknowledgedOffset{" + "topicPartition="
                + topicPartition + ", nextOffsetAndMetadata="
                + nextOffsetAndMetadata + '}';
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public OffsetAndMetadata nextOffsetAndMetadata() {
        return nextOffsetAndMetadata;
    }
}
