package io.atleon.kafka;

import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Mono;

import java.util.Optional;

/**
 * Per-partition tracker of processing based on offsets. Each instance is specific to a single
 * assignment lifecycle of a partition being consumed/processed. Non-trivial implementations may
 * keep track of record offsets that have been acknowledged, and expose that state as serializable
 * metadata. Implementations may also control native commitment of offsets with metadata to
 * associate with commitments, or disable native commitment altogether.
 */
public interface OffsetTracker {

    /**
     * Facilitates default offset tracking behavior, where no records are prohibited from
     * processing and commitment metadata is blank.
     */
    static OffsetTracker simple(TopicPartition partition) {
        return new Simple(partition);
    }

    /**
     * Facilitates effectively stateless/empty offset tracking behavior, where no records are
     * prohibited from processing and native commitment is skipped.
     */
    static OffsetTracker commitless(TopicPartition partition) {
        return new Commitless(partition);
    }

    /**
     * Indicates whether the provided offset identifies a record for which processing is
     * prohibited. This can be used to skip processing of targeted records, e.g. if they have been
     * previously acknowledged.
     */
    boolean prohibitsProcessing(long offset);

    /**
     * Hook for notification on acknowledgement of {@link ConsumerOffset} with given offset.
     */
    void acknowledged(long offset);

    /**
     * Invoked when commitment is being prepared, subscribed when commitment is executed. Returning
     * an empty {@link Mono} (e.g. {@code Mono.empty()})  will cause native commitment to be
     * skipped. Note that this is not the same as returning <i>blank</i> metadata (e.g.
     * {@code Mono.just("")}), which still triggers native commitment with that blank metadata.
     */
    Mono<String> commitMetadata(long commitOffset);

    /**
     * Initial offset that may be used as commitment offset, which is useful if commitment data may
     * update prior to a subsequent offset being made available for commit.
     */
    Optional<Long> initiallyCommittableOffset();

    TopicPartition topicPartition();

    final class Simple implements OffsetTracker {

        private final TopicPartition topicPartition;

        private Simple(TopicPartition partition) {
            this.topicPartition = partition;
        }

        @Override
        public boolean prohibitsProcessing(long offset) {
            return false;
        }

        @Override
        public void acknowledged(long offset) {}

        @Override
        public Mono<String> commitMetadata(long commitOffset) {
            return Mono.just("");
        }

        @Override
        public Optional<Long> initiallyCommittableOffset() {
            return Optional.empty();
        }

        @Override
        public TopicPartition topicPartition() {
            return topicPartition;
        }
    }

    final class Commitless implements OffsetTracker {

        private final TopicPartition topicPartition;

        private Commitless(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
        }

        @Override
        public boolean prohibitsProcessing(long offset) {
            return false;
        }

        @Override
        public void acknowledged(long offset) {}

        @Override
        public Mono<String> commitMetadata(long commitOffset) {
            return Mono.empty();
        }

        @Override
        public Optional<Long> initiallyCommittableOffset() {
            return Optional.empty();
        }

        @Override
        public TopicPartition topicPartition() {
            return topicPartition;
        }
    }
}
