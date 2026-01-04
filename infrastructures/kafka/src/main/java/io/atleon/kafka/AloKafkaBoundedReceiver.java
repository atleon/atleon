package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import io.atleon.core.ComposedAlo;
import io.atleon.core.ErrorEmitter;
import io.atleon.util.Consuming;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;

/**
 * Receiver of Kafka Records where the number of Records consumed is finite, and based on the
 * configured consumer group. This provides at-least-once semantics for discretely consuming
 * records and statefully tracking progress using the provided consumer group ID. Typical use cases
 * include:
 * - Periodically triggering "at-least-once" consumption of data
 * - Consuming data that has not yet been consumed by a group
 * <p>
 * Offsets are committed after the consumption of each range of records from a Topic-Partition has
 * been fully acknowledged. Each range must be fully acknowledged before the next range is
 * received.
 *
 * @param <K> inbound record key type
 * @param <V> inbound record value type
 */
public class AloKafkaBoundedReceiver<K, V> {

    private final KafkaConfigSource configSource;

    private AloKafkaBoundedReceiver(KafkaConfigSource configSource) {
        this.configSource = configSource;
    }

    public static <K, V> AloKafkaBoundedReceiver<K, V> create(KafkaConfigSource configSource) {
        return new AloKafkaBoundedReceiver<>(configSource);
    }

    /**
     * Creates a finite {@link AloFlux} of Kafka {@link ConsumerRecord}s and commits offsets for
     * the configured consumer group after finite consumption of each {@link TopicPartition} has
     * been fully acknowledged.
     *
     * @param topic        The topic to subscribe to
     * @param maxInclusive Where consumption should stop
     * @return An {@link AloFlux} of Kafka ConsumerRecords
     */
    public AloFlux<ConsumerRecord<K, V>> receiveAloRecordsUpTo(String topic, OffsetCriteria maxInclusive) {
        return receiveAloRecordsUpTo(Collections.singletonList(topic), maxInclusive);
    }

    /**
     * Creates a finite {@link AloFlux} of Kafka {@link ConsumerRecord}s and commits offsets for
     * the configured consumer group after finite consumption of each {@link TopicPartition} has
     * been fully acknowledged.
     *
     * @param topics       The topics to subscribe to
     * @param maxInclusive Where consumption should stop
     * @return An {@link AloFlux} of Kafka ConsumerRecords
     */
    public AloFlux<ConsumerRecord<K, V>> receiveAloRecordsUpTo(Collection<String> topics, OffsetCriteria maxInclusive) {
        return configSource
                .create()
                .flatMapMany(it -> receiveAloRecordsUpTo(it, topics, maxInclusive))
                .as(AloFlux::wrap);
    }

    private Flux<Alo<ConsumerRecord<K, V>>> receiveAloRecordsUpTo(
            KafkaConfig config, Collection<String> topics, OffsetCriteria maxInclusive) {
        return Flux.using(
                () -> ReactiveAdmin.create(config.nativeProperties()),
                it -> receiveAndCommitRecordsInRange(it, config, topics, maxInclusive),
                ReactiveAdmin::close);
    }

    private Flux<Alo<ConsumerRecord<K, V>>> receiveAndCommitRecordsInRange(
            ReactiveAdmin admin, KafkaConfig config, Collection<String> topics, OffsetCriteria maxInclusive) {
        String groupId = config.loadString(ConsumerConfig.GROUP_ID_CONFIG)
                .orElseThrow(() -> new IllegalArgumentException("Must provide Consumer Group ID"));
        OffsetResetStrategy resetStrategy = config.loadString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
                .map(it -> OffsetResetStrategy.valueOf(it.toUpperCase(Locale.ROOT)))
                .orElse(OffsetResetStrategy.LATEST); // Same default as Kafka Consumer Config
        return admin.listTopicPartitions(topics)
                .collectList()
                .flatMapMany(it -> admin.listTopicPartitionGroupOffsets(groupId, resetStrategy, it))
                .collectMap(
                        TopicPartitionGroupOffsets::topicPartition, it -> toOffsetRange(it.groupOffset(), maxInclusive))
                .flatMapMany(it -> RecordRange.list(admin, it))
                .filter(RecordRange::hasNonNegativeLength)
                .sort(Comparator.comparing(RecordRange::topicPartition, KafkaComparators.topicThenPartition()))
                .concatMap(it -> receiveAndCommitRecordsInRange(admin, groupId, it));
    }

    private AloFlux<ConsumerRecord<K, V>> receiveAndCommitRecordsInRange(
            ReactiveAdmin admin, String groupId, RecordRange recordRange) {
        Map<TopicPartition, Long> offsetsToCommit =
                Collections.singletonMap(recordRange.topicPartition(), recordRange.maxInclusive() + 1);
        ErrorEmitter<Alo<ConsumerRecord<K, V>>> errorEmitter = ErrorEmitter.create();
        Runnable acknowledger = () -> admin.alterRawConsumerGroupOffsets(groupId, offsetsToCommit)
                .subscribe(Consuming.noOp(), errorEmitter::safelyEmit, errorEmitter::safelyComplete);
        return AloFlux.just(new ComposedAlo<>(recordRange, acknowledger, errorEmitter::safelyEmit))
                .concatMap(KafkaBoundedReceiver.<K, V>create(configSource)::receiveRecordsInRange)
                .transform(errorEmitter::applyTo);
    }

    private static OffsetRange toOffsetRange(long rawMinInclusive, OffsetCriteria maxInclusive) {
        return OffsetCriteria.raw(rawMinInclusive).to(maxInclusive);
    }
}
