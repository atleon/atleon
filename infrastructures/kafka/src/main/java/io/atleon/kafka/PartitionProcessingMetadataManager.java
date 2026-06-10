package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Per-partition manager of processing metadata. Each instance is specific to a single assignment
 * lifecycle of a partition being consumed/processed. Non-trivial implementations may keep track of
 * record offsets that have been processed, and expose that state as serializable metadata.
 */
interface PartitionProcessingMetadataManager {

    static Map<TopicPartition, PartitionProcessingMetadataManager> create(
            int honorshipAheadOfCommit, Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        return honorshipAheadOfCommit <= 0 || partitions.isEmpty()
                ? Collections.emptyMap()
                : AheadOfCommit.create(honorshipAheadOfCommit, consumer, partitions);
    }

    static PartitionProcessingMetadataManager noOp() {
        return NoOp.INSTANCE;
    }

    void processed(long offset);

    String updateAndGetMetadataOnCommit(long commitOffset);

    boolean previouslyProcessed(long offset);

    Optional<Long> initialAcknowledgeableOffset();

    final class NoOp implements PartitionProcessingMetadataManager {

        private static final PartitionProcessingMetadataManager INSTANCE = new NoOp();

        private NoOp() {}

        @Override
        public void processed(long offset) {}

        @Override
        public String updateAndGetMetadataOnCommit(long commitOffset) {
            return "";
        }

        @Override
        public boolean previouslyProcessed(long offset) {
            return false;
        }

        @Override
        public Optional<Long> initialAcknowledgeableOffset() {
            return Optional.empty();
        }
    }

    final class AheadOfCommit implements PartitionProcessingMetadataManager {

        private final int honorship;

        private final long initialPosition;

        private final List<Long> sortedPreviouslyProcessedOffsets;

        private final Set<Long> processedOffsets;

        private AheadOfCommit(int honorship, long initialPosition, List<Long> sortedPreviouslyProcessedOffsets) {
            this.honorship = honorship;
            this.initialPosition = initialPosition;
            this.sortedPreviouslyProcessedOffsets = sortedPreviouslyProcessedOffsets;
            this.processedOffsets = ConcurrentHashMap.newKeySet();
            processedOffsets.addAll(sortedPreviouslyProcessedOffsets);
        }

        private static Map<TopicPartition, PartitionProcessingMetadataManager> create(
                int honorship, Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(new HashSet<>(partitions));
            return partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> {
                long initialPosition = consumer.position(partition);
                List<Long> sortedPreviouslyProcessedOffsets = Optional.ofNullable(committedOffsets.get(partition))
                        .map(it -> OffsetMetadataEncoding.decodeProcessed(initialPosition, it.metadata(), honorship))
                        .orElse(Collections.emptyList());
                return new AheadOfCommit(honorship, initialPosition, sortedPreviouslyProcessedOffsets);
            }));
        }

        @Override
        public void processed(long offset) {
            processedOffsets.add(offset);
        }

        @Override
        public String updateAndGetMetadataOnCommit(long commitOffset) {
            SortedSet<Long> processedOffsetsToEncode = new TreeSet<>();
            Iterator<Long> iterator = processedOffsets.iterator();
            while (iterator.hasNext()) {
                long processedOffset = iterator.next();
                if (processedOffset < commitOffset) {
                    iterator.remove();
                } else {
                    processedOffsetsToEncode.add(processedOffset);
                }
            }
            return OffsetMetadataEncoding.encodeProcessed(commitOffset, processedOffsetsToEncode, honorship);
        }

        @Override
        public boolean previouslyProcessed(long offset) {
            int previouslyProcessedOffsetsSize = sortedPreviouslyProcessedOffsets.size();
            return previouslyProcessedOffsetsSize > 0
                    && offset <= sortedPreviouslyProcessedOffsets.get(previouslyProcessedOffsetsSize - 1)
                    && Collections.binarySearch(sortedPreviouslyProcessedOffsets, offset) >= 0;
        }

        @Override
        public Optional<Long> initialAcknowledgeableOffset() {
            return Optional.of(initialPosition - 1);
        }
    }
}
