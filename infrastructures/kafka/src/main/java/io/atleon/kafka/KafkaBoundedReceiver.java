package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

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
        return configSource
                .create()
                .flatMapMany(it -> listRecordRanges(it, topics, rangeProvider))
                .filter(RecordRange::hasNonNegativeLength)
                .flatMap(this::receiveRecordsInRange, rangeProvider.maxConcurrentTopicPartitions());
    }

    Flux<ConsumerRecord<K, V>> receiveRecordsInRange(RecordRange recordRange) {
        return configSource.create().flatMapMany(it -> receiveRecordsInRange(it, recordRange));
    }

    private Flux<ConsumerRecord<K, V>> receiveRecordsInRange(KafkaConfig kafkaConfig, RecordRange recordRange) {
        KafkaReceiverOptions<K, V> receiverOptions = KafkaReceiverOptions.<K, V>newBuilder()
                .consumerListener(ConsumerListener.seekOnce(recordRange.topicPartition(), recordRange.minInclusive()))
                .consumerProperties(kafkaConfig.nativeProperties())
                .commitlessOffsets(true)
                .pollTimeout(kafkaConfig.loadDuration(POLL_TIMEOUT_CONFIG).orElse(DEFAULT_POLL_TIMEOUT))
                .closeTimeout(kafkaConfig.loadDuration(CLOSE_TIMEOUT_CONFIG).orElse(DEFAULT_CLOSE_TIMEOUT))
                .build();

        return KafkaReceiver.create(receiverOptions)
                .receiveAutoAckWithAssignment(Collections.singletonList(recordRange.topicPartition()))
                .concatMap(Function.identity())
                .takeWhile(it -> it.offset() <= recordRange.maxInclusive())
                .takeUntil(it -> it.offset() == recordRange.maxInclusive());
    }

    private static Flux<RecordRange> listRecordRanges(
            KafkaConfig config, Collection<String> topics, OffsetRangeProvider rangeProvider) {
        return Flux.using(
                () -> ReactiveAdmin.create(config.nativeProperties()),
                it -> RecordRange.list(it, topics, rangeProvider),
                ReactiveAdmin::close);
    }
}
