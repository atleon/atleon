package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.function.LongFunction;

/**
 * A {@link ConsumerOffset} that has been acknowledged and is eligible to be used as a basis for
 * commitment. Note that the contained offset itself is <i>not</i> what is committed, but rather
 * the actual offset to commit is the numerical increment of that offset, along with (lazily
 * evaluated) metadata.
 */
final class AcknowledgedOffset {

    private final ConsumerOffset consumerOffset;

    private final LongFunction<String> offsetCommitmentMetadataFactory;

    public AcknowledgedOffset(ConsumerOffset consumerOffset, LongFunction<String> offsetCommitmentMetadataFactory) {
        this.consumerOffset = consumerOffset;
        this.offsetCommitmentMetadataFactory = offsetCommitmentMetadataFactory;
    }

    public TopicPartition topicPartition() {
        return consumerOffset.topicPartition();
    }

    public ConsumerOffset consumerOffset() {
        return consumerOffset;
    }

    public OffsetAndMetadata nextOffset() {
        return nextOffsetAndMetadata(__ -> "");
    }

    public OffsetAndMetadata commitOffset() {
        return nextOffsetAndMetadata(offsetCommitmentMetadataFactory);
    }

    private OffsetAndMetadata nextOffsetAndMetadata(LongFunction<String> nextOffsetMetadataFactory) {
        long nextOffset = consumerOffset.offset() + 1;
        String metadata = nextOffsetMetadataFactory.apply(nextOffset);
        return new OffsetAndMetadata(nextOffset, consumerOffset.leaderEpoch(), metadata);
    }
}
