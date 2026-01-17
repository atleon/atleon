package io.atleon.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * When describing the current state of a Topic, each {@link TopicPartition} is passed to an
 * implementation of this type in order to describe the range of Records that should be consumed.
 * In addition, this interface provides a {@link Comparator} used to order TopicPartitions such
 * that their order of consumption is deterministic. By default, this ordering is by the name of
 * the topic, then the partition number.
 */
@FunctionalInterface
public interface OffsetRangeProvider {

    static Builder newBuilder() {
        return new Builder();
    }

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
        return new Builder().inOffsetRangeFromAllTopicPartitions(offsetRange).build();
    }

    /**
     * Consume Records starting from a "raw" offset to the offset that matches the max Criteria
     */
    static OffsetRangeProvider inOffsetRangeFromTopicPartition(
            TopicPartition topicPartition, long rawOffset, OffsetCriteria maxInclusive) {
        return inOffsetRangeFromTopicPartition(
                topicPartition, OffsetCriteria.raw(rawOffset).to(maxInclusive));
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
        return new Builder().inOffsetRangesFromTopicPartitions(offsetRanges).build();
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
    static OffsetRangeProvider usingRawOffsetRanges(
            Map<TopicPartition, RawOffsetRange> rawOffsetRangesByTopicPartition) {
        return new Builder()
                .usingRawOffsetRanges(rawOffsetRangesByTopicPartition)
                .build();
    }

    /**
     * Starting from a provided TopicPartition and "raw" offset, continue consuming Records from
     * there through the subsequent TopicPartitions matching the provided OffsetRange. Note that
     * "subsequent" is determined by the default Comparator (sort by Topic, then Partition)
     */
    static OffsetRangeProvider startingFromRawOffsetInTopicPartition(
            TopicPartition topicPartition, long rawOffset, OffsetRange offsetRange) {
        return new StartingFromRawOffsetInTopicPartition(topicPartition, rawOffset, offsetRange);
    }

    Optional<OffsetRange> forTopicPartition(TopicPartition topicPartition);

    /**
     * Returns a Comparator used to sort TopicPartitions such that Records are consumed in a
     * deterministic order
     */
    default Comparator<? super TopicPartition> topicPartitionComparator() {
        return KafkaComparators.topicThenPartition();
    }

    /**
     * Returns the maximum number of partitions that may be concurrently polled
     */
    default int maxConcurrentTopicPartitions() {
        return 1;
    }

    final class Builder {

        private Function<TopicPartition, Optional<OffsetRange>> topicPartitionToRange = __ -> Optional.empty();

        private Comparator<? super TopicPartition> topicPartitionComparator = KafkaComparators.topicThenPartition();

        private int maxConcurrentTopicPartitions = 1;

        private Builder() {}

        /**
         * Consume all Records across all TopicPartitions from the earliest to latest (at time of
         * describing the Topic)
         */
        public Builder earliestToLatestFromAllTopicPartitions() {
            return inOffsetRangeFromAllTopicPartitions(OffsetCriteria.earliest().to(OffsetCriteria.latest()));
        }

        /**
         * Consume Records across all TopicPartitions within the provided OffsetRange
         */
        public Builder inOffsetRangeFromAllTopicPartitions(OffsetRange offsetRange) {
            return topicPartitionToRange(__ -> Optional.of(offsetRange));
        }

        /**
         * Consume Records starting from a "raw" offset to the offset that matches the max Criteria
         */
        public Builder inOffsetRangeFromTopicPartition(
                TopicPartition topicPartition, long rawOffset, OffsetCriteria maxInclusive) {
            return inOffsetRangeFromTopicPartition(
                    topicPartition, OffsetCriteria.raw(rawOffset).to(maxInclusive));
        }

        /**
         * Consume Records from a single TopicPartition within the provided OffsetRange
         */
        public Builder inOffsetRangeFromTopicPartition(TopicPartition topicPartition, OffsetRange offsetRange) {
            return inOffsetRangesFromTopicPartitions(Collections.singletonMap(topicPartition, offsetRange));
        }

        /**
         * Consume Records from TopicPartitions within mapped OffsetRanges
         */
        public Builder inOffsetRangesFromTopicPartitions(Map<TopicPartition, OffsetRange> offsetRanges) {
            return topicPartitionToRange(it -> Optional.ofNullable(offsetRanges.get(it)));
        }

        /**
         * Consume Records from a single TopicPartition that are in the provided "raw" Offset range
         */
        public Builder usingRawOffsetRange(TopicPartition topicPartition, RawOffsetRange rawOffsetRange) {
            return usingRawOffsetRanges(Collections.singletonMap(topicPartition, rawOffsetRange));
        }

        /**
         * Consume Records from provided TopicPartitions that are in the provided mapped "raw"
         * Offset ranges
         */
        public Builder usingRawOffsetRanges(Map<TopicPartition, RawOffsetRange> rawOffsetRangesByTopicPartition) {
            return topicPartitionToRange(it ->
                    Optional.ofNullable(rawOffsetRangesByTopicPartition.get(it)).map(RawOffsetRange::toOffsetRange));
        }

        /**
         * Specify a function that returns an optional range of offsets to consume on a
         * per-partition basis
         */
        public Builder topicPartitionToRange(Function<TopicPartition, Optional<OffsetRange>> topicPartitionToRange) {
            this.topicPartitionToRange = topicPartitionToRange;
            return this;
        }

        /**
         * Set the comparator that is used to determine the order of partition consumption. Mainly
         * useful for deterministic consumption and facilitating resumption processes when using
         * single concurrency.
         */
        public Builder topicPartitionComparator(Comparator<? super TopicPartition> topicPartitionComparator) {
            this.topicPartitionComparator = topicPartitionComparator;
            return this;
        }

        /**
         * Specify the maximum number of partitions to poll/consume concurrently. A concurrency of
         * 1 helps to facilitate any necessary resumption, whereas a higher concurrency results in
         * more even distribution of load. Setting this to {@link Integer#MAX_VALUE} is valid, and
         * will result in polling all available partitions concurrently.
         */
        public Builder maxConcurrentTopicPartitions(int maxConcurrentTopicPartitions) {
            this.maxConcurrentTopicPartitions = maxConcurrentTopicPartitions;
            return this;
        }

        public OffsetRangeProvider build() {
            return new Composed(topicPartitionToRange, topicPartitionComparator, maxConcurrentTopicPartitions);
        }
    }

    final class Composed implements OffsetRangeProvider {

        private final Function<TopicPartition, Optional<OffsetRange>> topicPartitionToRange;

        private final Comparator<? super TopicPartition> topicPartitionComparator;

        private final int maxConcurrentTopicPartitions;

        private Composed(
                Function<TopicPartition, Optional<OffsetRange>> topicPartitionToRange,
                Comparator<? super TopicPartition> topicPartitionComparator,
                int maxConcurrentTopicPartitions) {
            this.topicPartitionToRange = topicPartitionToRange;
            this.topicPartitionComparator = topicPartitionComparator;
            this.maxConcurrentTopicPartitions = maxConcurrentTopicPartitions;
        }

        @Override
        public Optional<OffsetRange> forTopicPartition(TopicPartition topicPartition) {
            return topicPartitionToRange.apply(topicPartition);
        }

        @Override
        public Comparator<? super TopicPartition> topicPartitionComparator() {
            return topicPartitionComparator;
        }

        @Override
        public int maxConcurrentTopicPartitions() {
            return maxConcurrentTopicPartitions;
        }
    }

    final class StartingFromRawOffsetInTopicPartition implements OffsetRangeProvider {

        private final TopicPartition startingTopicPartition;

        private final long startingRawOffset;

        private final OffsetRange offsetRange;

        private StartingFromRawOffsetInTopicPartition(
                TopicPartition topicPartition, long rawOffset, OffsetRange offsetRange) {
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
