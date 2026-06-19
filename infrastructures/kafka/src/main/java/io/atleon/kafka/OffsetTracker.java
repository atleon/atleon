package io.atleon.kafka;

import io.atleon.core.SerialQueue;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Per-partition tracker of processing based on offsets. Each instance is specific to a single
 * assignment lifecycle of a partition being consumed/processed. Non-trivial implementations may
 * keep track of record offsets that have been acknowledged, and expose that state as serializable
 * metadata. Implementations may also control native commitment of offsets with metadata to
 * associate with commitments, or disable native commitment altogether.
 */
public interface OffsetTracker {

    int DEFAULT_METADATA_MAX_BYTES = 4096; // Kafka default

    int DEFAULT_BUFFER_MAX_SIZE = 10_000;

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
     * @see #acknowledgedAheadOfCommit(ConsumerPartition, int, int)
     */
    static OffsetTracker acknowledgedAheadOfCommit(ConsumerPartition partition) {
        return acknowledgedAheadOfCommit(partition, DEFAULT_METADATA_MAX_BYTES, DEFAULT_BUFFER_MAX_SIZE);
    }

    /**
     * Activates tracking of acknowledged offsets which are ahead of the committed offset. This
     * results in queue-like behavior by storing acknowledged offsets in commit metadata, serving
     * as a non-volatile record (across rebalances) of processing that has completed ahead of the
     * offset that is safe to commit at commit time. This method allows configuring the max
     * serialized size of metadata (limited by broker-side {@code offset.metadata.max.bytes}), and
     * the maximum number of "buffered" offsets, which helps control memory usage.
     */
    static OffsetTracker acknowledgedAheadOfCommit(
            ConsumerPartition partition, int metadataMaxBytes, int bufferMaxSize) {
        return new AcknowledgedAheadOfCommit(partition, metadataMaxBytes, bufferMaxSize);
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
     * Initial {@link ConsumerOffset} that may be used as a basis for commitment, in the absence of
     * commitment offset advancement. This is useful if commitment metadata may update prior to a
     * subsequent offset being made available for commitment. Generally, the contained raw offset
     * value should be one less than the initial position/committed offset.
     */
    Optional<ConsumerOffset> initialConsumerOffset();

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
        public Optional<ConsumerOffset> initialConsumerOffset() {
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
        public Optional<ConsumerOffset> initialConsumerOffset() {
            return Optional.empty();
        }

        @Override
        public TopicPartition topicPartition() {
            return topicPartition;
        }
    }

    final class AcknowledgedAheadOfCommit implements OffsetTracker {

        private final ConsumerOffset initialConsumerOffset;

        private final OffsetMetadataEncoding metadataEncoding;

        private final int bufferMaxSize;

        private final SerialQueue<Consumer<Set<Long>>> acknowledgedQueue = SerialQueue.on(new HashSet<>());

        // Written only under the `acknowledgedQueue` (serialized RMW), but read on the poll thread
        // in prohibitsProcessing without synchronization. Intentionally NOT volatile, since we
        // assume prohibition only needs the initially decoded set, and in-session offsets are
        // assumed to monotonically increase with processing. A stale read on the poll thread is
        // therefore harmless. Reconsider making this volatile if this non-local invariant ever
        // changes (e.g. prohibition must reflect in-session acknowledgements).
        private Offsets aheadOfCommit;

        private AcknowledgedAheadOfCommit(ConsumerPartition partition, int metadataMaxSize, int bufferMaxSize) {
            this.initialConsumerOffset = partition.newOffsetFromPosition(it -> it - 1);
            this.metadataEncoding = new OffsetMetadataEncoding(metadataMaxSize);
            this.bufferMaxSize = bufferMaxSize;
            this.aheadOfCommit = partition
                    .committedMetadata()
                    .map(it -> metadataEncoding.decode(initialConsumerOffset.offset(), it))
                    .orElseGet(Offsets::new);
        }

        @Override
        public boolean prohibitsProcessing(long offset) {
            return aheadOfCommit.contains(offset);
        }

        @Override
        public void acknowledged(long offset) {
            acknowledgedQueue.addAndDrain(it -> {
                if (it.add(offset) && it.size() >= bufferMaxSize) {
                    drainAcknowledgedBuffer(it, Long.MIN_VALUE);
                }
            });
        }

        @Override
        public Mono<String> commitMetadata(long commitOffset) {
            return Mono.<Offsets>create(sink -> drainAcknowledgedBuffer(sink, commitOffset))
                    .map(it -> metadataEncoding.encode(commitOffset - 1, it));
        }

        @Override
        public Optional<ConsumerOffset> initialConsumerOffset() {
            return Optional.of(initialConsumerOffset);
        }

        @Override
        public TopicPartition topicPartition() {
            return initialConsumerOffset.topicPartition();
        }

        private void drainAcknowledgedBuffer(MonoSink<Offsets> sink, long minimumOffset) {
            acknowledgedQueue.addAndDrain(it -> {
                drainAcknowledgedBuffer(it, minimumOffset);
                sink.success(aheadOfCommit);
            });
        }

        private void drainAcknowledgedBuffer(Set<Long> acknowledgedBuffer, long minimumOffset) {
            SortedSet<Long> bufferedAheadOfCommit = acknowledgedBuffer.stream()
                    .filter(it -> it >= minimumOffset)
                    .collect(Collectors.toCollection(TreeSet::new));

            aheadOfCommit = aheadOfCommit.minimumMerge(minimumOffset, bufferedAheadOfCommit);
            acknowledgedBuffer.clear();
        }
    }
}
