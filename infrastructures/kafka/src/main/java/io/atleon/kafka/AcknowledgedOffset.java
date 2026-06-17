package io.atleon.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.function.LongFunction;

/**
 * A {@link ConsumerOffset} that has been acknowledged and is eligible to be used as a basis for
 * commitment. Note that the contained offset itself is <i>not</i> what is committed, but rather
 * the actual offset to commit is the numerical increment of that offset, along with (lazily
 * evaluated) metadata.
 */
final class AcknowledgedOffset {

    private final ConsumerOffset consumerOffset;

    private final LongFunction<Mono<String>> metadataCommitment;

    public AcknowledgedOffset(ConsumerOffset consumerOffset, LongFunction<Mono<String>> metadataCommitment) {
        this.consumerOffset = consumerOffset;
        this.metadataCommitment = metadataCommitment;
    }

    public Mono<Tuple2<TopicPartition, OffsetAndMetadata>> prepareCommitment() {
        long commitOffset = consumerOffset.offset() + 1;
        Mono<String> commitMetadata = metadataCommitment.apply(commitOffset);
        return commitMetadata.map(it -> Tuples.of(topicPartition(), newOffsetAndMetadata(commitOffset, it)));
    }

    public TopicPartition topicPartition() {
        return consumerOffset.topicPartition();
    }

    public ConsumerOffset consumerOffset() {
        return consumerOffset;
    }

    private OffsetAndMetadata newOffsetAndMetadata(long offset, String metadata) {
        return new OffsetAndMetadata(offset, consumerOffset.leaderEpoch(), metadata);
    }
}
