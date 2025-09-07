package io.atleon.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

/**
 * When describing the current state of a Topic, each {@link TopicPartition} is passed to an
 * implementation of this type in order to describe the range of Records that should be consumed.
 * In addition, this interface provides a {@link Comparator} used to order TopicPartitions such
 * that their order of consumption is deterministic. By default, this ordering is by the name of
 * the topic, then the partition number.
 */
@FunctionalInterface
public interface OffsetRangeProvider {

    /**
     * Consume all Records across all TopicPartitions from the earliest to latest (at time of
     * describing the Topic)
     */
    static OffsetRangeProvider earliestToLatestFromAllTopicPartitions() {
        return inOffsetRangeFromAllTopicPartitions(OffsetCriteria.earliest().to(OffsetCriteria.latest()));
    }

    /**
     * Consume Records across all TopicPartitions within the provided OffsetRange
     */
    static OffsetRangeProvider inOffsetRangeFromAllTopicPartitions(OffsetRange offsetRange) {
        return __ -> Optional.of(offsetRange);
    }

    /**
     * Consume Records from a single TopicPartition within the provided OffsetRange
     */
    static OffsetRangeProvider inOffsetRangeFromTopicPartition(TopicPartition topicPartition, OffsetRange offsetRange) {
        return inOffsetRangesFromTopicPartitions(Collections.singletonMap(topicPartition, offsetRange));
    }

    /**
     * Consume Records from TopicPartitions within mapped OffsetRanges
     */
    static OffsetRangeProvider inOffsetRangesFromTopicPartitions(Map<TopicPartition, OffsetRange> offsetRanges) {
        return it -> Optional.ofNullable(offsetRanges.get(it));
    }

    /**
     * Consume Records starting from a "raw" offset to the offset that matches the max Criteria
     */
    static OffsetRangeProvider inOffsetRangeFromTopicPartition(
        TopicPartition topicPartition,
        long rawOffset,
        OffsetCriteria maxInclusive
    ) {
        return inOffsetRangeFromTopicPartition(topicPartition, OffsetCriteria.raw(rawOffset).to(maxInclusive));
    }

    /**
     * Starting from a provided TopicPartition and "raw" offset, continue consuming Records from
     * there through the subsequent TopicPartitions matching the provided OffsetRange. Note that
     * "subsequent" is determined by the default Comparator (sort by Topic, then Partition)
     */
    static OffsetRangeProvider startingFromRawOffsetInTopicPartition(
        TopicPartition topicPartition,
        long rawOffset,
        OffsetRange offsetRange
    ) {
        return new StartingFromRawOffsetInTopicPartition(topicPartition, rawOffset, offsetRange);
    }

    /**
     * Consume Records from a single TopicPartition that are in the provided "raw" Offset range
     */
    static OffsetRangeProvider usingRawOffsetRange(TopicPartition topicPartition, RawOffsetRange rawOffsetRange) {
        return usingRawOffsetRanges(Collections.singletonMap(topicPartition, rawOffsetRange));
    }

    /**
     * Consume Records from provided TopicPartitions that are in the provided mapped "raw" Offset
     * ranges
     */
    static OffsetRangeProvider usingRawOffsetRanges(Map<TopicPartition, RawOffsetRange> rawOffsetRangesByTopicPartition) {
        return it -> Optional.ofNullable(rawOffsetRangesByTopicPartition.get(it)).map(RawOffsetRange::toOffsetRange);
    }

    Optional<OffsetRange> forTopicPartition(TopicPartition topicPartition);

    /**
     * Returns a Comparator used to sort TopicPartitions such that Records are consumed in a
     * deterministic order
     */
    default Comparator<? super TopicPartition> topicPartitionComparator() {
        return KafkaComparators.topicThenPartition();
    }

    final class StartingFromRawOffsetInTopicPartition implements OffsetRangeProvider {

        private final TopicPartition startingTopicPartition;

        private final long startingRawOffset;

        private final OffsetRange offsetRange;

        private StartingFromRawOffsetInTopicPartition(
            TopicPartition topicPartition,
            long rawOffset,
            OffsetRange offsetRange
        ) {
            this.startingTopicPartition = topicPartition;
            this.startingRawOffset = rawOffset;
            this.offsetRange = offsetRange;
        }

        @Override
        public Optional<OffsetRange> forTopicPartition(TopicPartition topicPartition) {
            int comparison = topicPartitionComparator().compare(startingTopicPartition, topicPartition);
            if (comparison == 0) {
                return Optional.of(OffsetCriteria.raw(startingRawOffset).to(offsetRange.maxInclusive()));
            } else {
                return comparison < 0 ? Optional.of(offsetRange) : Optional.empty();
            }
        }
    }
}
