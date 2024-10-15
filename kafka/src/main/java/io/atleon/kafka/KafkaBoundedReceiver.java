package io.atleon.kafka;

import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
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

/**
 * Receiver of Kafka Records where the number of Records consumed is finite as indicated by
 * Offset Range Providers. Typical use cases include:
 * - Get all Records from earliest to latest (at time of subscription)
 * - Get all Records produced in a given timespan
 * - Get all Records in a given "raw" offset range
 * - Get all Records starting/resuming from specific partition and offset
 */
public class KafkaBoundedReceiver<K, V> {

    /**
     * Prefix used on all KafkaRangeReceiver-specific configurations
     */
    public static final String CONFIG_PREFIX = "kafka.bounded.receiver";

    /**
     * Controls timeouts of polls to Kafka. This config can be increased if a Kafka cluster is
     * slow to respond. Specified as ISO-8601 Duration, e.g. PT10S
     */
    public static final String POLL_TIMEOUT_CONFIG = CONFIG_PREFIX + "poll.timeout";

    /**
     * Closing the underlying Kafka Consumer is a fallible process. In order to not infinitely
     * deadlock a Consumer during this process (which can lead to non-consumption of assigned
     * partitions), we use a default equal to what's used in KafkaConsumer::close
     */
    public static final String CLOSE_TIMEOUT_CONFIG = CONFIG_PREFIX + "close.timeout";

    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100L);

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30L);

    private final KafkaConfigSource configSource;

    private KafkaBoundedReceiver(KafkaConfigSource configSource) {
        this.configSource = configSource;
    }

    public static <K, V> KafkaBoundedReceiver<K, V> create(KafkaConfigSource configSource) {
        return new KafkaBoundedReceiver<>(configSource);
    }

    /**
     * Receive records from a given topic in the provided relative offset range.
     *
     * @param topic       The name of a topic to receive records from
     * @param offsetRange The relative range of offsets to receive
     * @return A bounded {@link Flux} of {@link ConsumerRecord}
     */
    public Flux<ConsumerRecord<K, V>> receiveRecords(String topic, OffsetRange offsetRange) {
        return receiveRecords(Collections.singletonList(topic), offsetRange);
    }

    /**
     * Receive records from given topics in the provided relative offset range.
     *
     * @param topics      The topic names to receive records from
     * @param offsetRange The relative range of offsets to receive
     * @return A bounded {@link Flux} of {@link ConsumerRecord}
     */
    public Flux<ConsumerRecord<K, V>> receiveRecords(Collection<String> topics, OffsetRange offsetRange) {
        return receiveRecords(topics, OffsetRangeProvider.inOffsetRangeFromAllTopicPartitions(offsetRange));
    }

    /**
     * Receive records from a given topic based on relative offset ranges provided by the given
     * {@link OffsetRangeProvider}.
     *
     * @param topic         The topic to receive records from
     * @param rangeProvider Provider of relative offset ranges to receive, given {@link TopicPartition}s
     * @return A bounded {@link Flux} of {@link ConsumerRecord}
     */
    public Flux<ConsumerRecord<K, V>> receiveRecords(String topic, OffsetRangeProvider rangeProvider) {
        return receiveRecords(Collections.singletonList(topic), rangeProvider);
    }

    /**
     * Receive records from given topics based on relative offset ranges provided by the given
     * {@link OffsetRangeProvider}.
     *
     * @param topics        The topic names to receive records from
     * @param rangeProvider Provider of relative offset ranges to receive, given {@link TopicPartition}s
     * @return A bounded {@link Flux} of {@link ConsumerRecord}
     */
    public Flux<ConsumerRecord<K, V>> receiveRecords(Collection<String> topics, OffsetRangeProvider rangeProvider) {
        return configSource.create().flatMapMany(it -> receiveRecords(it, topics, rangeProvider));
    }

    private Flux<ConsumerRecord<K, V>> receiveRecords(
        KafkaConfig config,
        Collection<String> topics,
        OffsetRangeProvider rangeProvider
    ) {
        Flux<RecordRange> recordRanges = Flux.using(
            () -> ReactiveAdmin.create(config.nativeProperties()),
            it -> listRecordRanges(it, topics, rangeProvider),
            ReactiveAdmin::close
        );
        return recordRanges
            .filter(RecordRange::hasNonNegativeLength)
            .concatMap(this::receiveRecordsInRange);
    }

    private Flux<RecordRange> listRecordRanges(
        ReactiveAdmin admin,
        Collection<String> topics,
        OffsetRangeProvider rangeProvider
    ) {
        return admin.listTopicPartitions(topics)
            .collectMap(Function.identity(), rangeProvider::forTopicPartition)
            .map(it -> sortPresent(it, rangeProvider.topicPartitionComparator()))
            .flatMapMany(it -> listRecordRanges(admin, it));
    }

    private Flux<RecordRange> listRecordRanges(ReactiveAdmin admin, SortedMap<TopicPartition, OffsetRange> ranges) {
        Map<TopicPartition, OffsetCriteria> minCriteria = ranges.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, it -> it.getValue().minInclusive()));
        Map<TopicPartition, OffsetCriteria> maxCriteria = ranges.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, it -> it.getValue().maxInclusive()));

        return Mono.zip(
            calculateRawOffsets(admin, minCriteria, Extrema.MIN),
            calculateRawOffsets(admin, maxCriteria, Extrema.MAX),
            admin.listOffsets(minCriteria.keySet(), OffsetSpec.earliest()),
            admin.listOffsets(maxCriteria.keySet(), OffsetSpec.latest())
        ).flatMapIterable(it -> createRecordRanges(ranges.keySet(), it.getT1(), it.getT2(), it.getT3(), it.getT4()));
    }

    private Mono<Map<TopicPartition, Long>> calculateRawOffsets(
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

    private Flux<ConsumerRecord<K, V>> receiveRecordsInRange(RecordRange recordRange) {
        return configSource.create().flatMapMany(it -> receiveRecordsInRange(it, recordRange));
    }

    private Flux<ConsumerRecord<K, V>> receiveRecordsInRange(KafkaConfig kafkaConfig, RecordRange recordRange) {
        ReceiverOptions<K, V> receiverOptions = ReceiverOptions.<K, V>create(kafkaConfig.nativeProperties())
            .assignment(Collections.singletonList(recordRange.topicPartition()))
            .pollTimeout(kafkaConfig.loadDuration(POLL_TIMEOUT_CONFIG).orElse(DEFAULT_POLL_TIMEOUT))
            .closeTimeout(kafkaConfig.loadDuration(CLOSE_TIMEOUT_CONFIG).orElse(DEFAULT_CLOSE_TIMEOUT))
            .addAssignListener(partitions -> partitions.forEach(it -> it.seek(recordRange.minInclusive())));

        return KafkaReceiver.create(receiverOptions).receive()
            .<ConsumerRecord<K, V>>map(Function.identity())
            .takeWhile(it -> it.offset() <= recordRange.maxInclusive())
            .takeUntil(it -> it.offset() == recordRange.maxInclusive());
    }

    private static List<RecordRange> createRecordRanges(
        Collection<TopicPartition> topicPartitions,
        Map<TopicPartition, Long> minOffsets,
        Map<TopicPartition, Long> maxOffsets,
        Map<TopicPartition, Long> earliestOffsets,
        Map<TopicPartition, Long> latestOffsets
    ) {
        return topicPartitions.stream()
            .map(topicPartition -> new RecordRange(
                topicPartition,
                Math.max(earliestOffsets.get(topicPartition), minOffsets.get(topicPartition)),
                Math.min(latestOffsets.get(topicPartition), maxOffsets.get(topicPartition)) - 1))
            .collect(Collectors.toList());
    }

    private static <K, V> SortedMap<K, V> sortPresent(Map<K, Optional<V>> map, Comparator<? super K> comparator) {
        return map.entrySet().stream()
            .filter(it -> it.getValue().isPresent())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                it -> it.getValue().get(),
                (l, r) -> l, // Won't be called, since we know the keys are unique
                () -> new TreeMap<>(comparator))
            );
    }

    private static long toRawOffset(OffsetCriteria endpoint, Extrema extrema) {
        return OffsetCriteria.Raw.class.cast(endpoint).offset() + (extrema == Extrema.MAX ? 1 : 0);
    }

    private static OffsetSpec toTimestampSpec(OffsetCriteria offsetCriteria, Extrema extrema) {
        OffsetCriteria.Timestamp timestampEndpoint = OffsetCriteria.Timestamp.class.cast(offsetCriteria);
        return OffsetSpec.forTimestamp(timestampEndpoint.epochMillis() + (extrema == Extrema.MAX ? 1 : 0));
    }

    private enum Extrema {MIN, MAX}

    private static final class RecordRange {

        private final TopicPartition topicPartition;

        private final long minInclusive;

        private final long maxInclusive;

        public RecordRange(TopicPartition topicPartition, long minInclusive, long maxExclusive) {
            this.topicPartition = topicPartition;
            this.minInclusive = minInclusive;
            this.maxInclusive = maxExclusive;
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
    }
}
