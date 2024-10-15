package io.atleon.kafka;

import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

final class RecordRange {

    private final TopicPartition topicPartition;

    private final long minInclusive;

    private final long maxInclusive;

    public RecordRange(TopicPartition topicPartition, long minInclusive, long maxExclusive) {
        this.topicPartition = topicPartition;
        this.minInclusive = minInclusive;
        this.maxInclusive = maxExclusive;
    }

    public static Flux<RecordRange> list(ReactiveAdmin admin, Collection<String> topics, OffsetRangeProvider rangeProvider) {
        return admin.listTopicPartitions(topics)
            .collectMap(Function.identity(), rangeProvider::forTopicPartition)
            .map(it -> sortPresent(it, rangeProvider.topicPartitionComparator()))
            .flatMapMany(it -> list(admin, it));
    }

    public static Flux<RecordRange> list(ReactiveAdmin admin, Map<TopicPartition, OffsetRange> ranges) {
        Map<TopicPartition, OffsetCriteria> minCriteria = ranges.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, it -> it.getValue().minInclusive()));
        Map<TopicPartition, OffsetCriteria> maxCriteria = ranges.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, it -> it.getValue().maxInclusive()));

        return Mono.zip(
                calculateRawOffsets(admin, minCriteria, Extrema.MIN),
                calculateRawOffsets(admin, maxCriteria, Extrema.MAX),
                admin.listOffsets(minCriteria.keySet(), OffsetSpec.earliest()),
                admin.listOffsets(maxCriteria.keySet(), OffsetSpec.latest()))
            .flatMapIterable(it -> createRecordRanges(ranges.keySet(), it.getT1(), it.getT2(), it.getT3(), it.getT4()));
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public boolean hasNonNegativeLength() {
        return maxInclusive - minInclusive >= 0;
    }

    public long minInclusive() {
        return minInclusive;
    }

    public long maxInclusive() {
        return maxInclusive;
    }

    private static Mono<Map<TopicPartition, Long>> calculateRawOffsets(
        ReactiveAdmin admin,
        Map<TopicPartition, OffsetCriteria> criteria,
        Extrema extrema
    ) {
        Map<Class<? extends OffsetCriteria>, Map<TopicPartition, OffsetCriteria>> typedCriteria =
            criteria.entrySet().stream().collect(Collectors.groupingBy(it -> it.getValue().getClass(),
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        Map<TopicPartition, Long> rawOffsets =
            typedCriteria.getOrDefault(OffsetCriteria.Raw.class, Collections.emptyMap()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, it -> toRawOffset(it.getValue(), extrema)));
        Map<TopicPartition, OffsetSpec> timestampSpecs =
            typedCriteria.getOrDefault(OffsetCriteria.Timestamp.class, Collections.emptyMap()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, it -> toTimestampSpec(it.getValue(), extrema)));
        Map<TopicPartition, OffsetSpec> earliestSpecs =
            typedCriteria.getOrDefault(OffsetCriteria.Earliest.class, Collections.emptyMap()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, __ -> OffsetSpec.earliest()));
        Map<TopicPartition, OffsetSpec> latestSpecs =
            typedCriteria.getOrDefault(OffsetCriteria.Latest.class, Collections.emptyMap()).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, __ -> OffsetSpec.latest()));

        return Mono.just(rawOffsets)
            .mergeWith(admin.listOffsets(timestampSpecs, offset -> offset == -1 ? Long.MAX_VALUE : offset))
            .mergeWith(admin.listOffsets(earliestSpecs, offset -> offset + (extrema == Extrema.MAX ? 1 : 0)))
            .mergeWith(admin.listOffsets(latestSpecs, offset -> offset - (extrema == Extrema.MIN ? 1 : 0)))
            .flatMapIterable(Map::entrySet)
            .collectMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    private static List<RecordRange> createRecordRanges(
        Collection<TopicPartition> topicPartitions,
        Map<TopicPartition, Long> minOffsets,
        Map<TopicPartition, Long> maxOffsets,
        Map<TopicPartition, Long> earliestOffsets,
        Map<TopicPartition, Long> latestOffsets
    ) {
        return topicPartitions.stream()
            .filter(it -> minOffsets.containsKey(it) && maxOffsets.containsKey(it))
            .map(topicPartition -> new RecordRange(
                topicPartition,
                Math.max(earliestOffsets.get(topicPartition), minOffsets.get(topicPartition)),
                Math.min(latestOffsets.get(topicPartition), maxOffsets.get(topicPartition)) - 1))
            .collect(Collectors.toList());
    }

    private static long toRawOffset(OffsetCriteria endpoint, Extrema extrema) {
        return OffsetCriteria.Raw.class.cast(endpoint).offset() + (extrema == Extrema.MAX ? 1 : 0);
    }

    private static OffsetSpec toTimestampSpec(OffsetCriteria offsetCriteria, Extrema extrema) {
        OffsetCriteria.Timestamp timestampEndpoint = OffsetCriteria.Timestamp.class.cast(offsetCriteria);
        return OffsetSpec.forTimestamp(timestampEndpoint.epochMillis() + (extrema == Extrema.MAX ? 1 : 0));
    }

    private static <K, V> SortedMap<K, V> sortPresent(Map<K, Optional<V>> map, Comparator<? super K> comparator) {
        return map.entrySet().stream()
            .filter(it -> it.getValue().isPresent())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                it -> it.getValue().get(),
                (l, r) -> l, // Won't be called, since we know the keys are unique
                () -> new TreeMap<>(comparator)));
    }

    private enum Extrema {MIN, MAX}
}
