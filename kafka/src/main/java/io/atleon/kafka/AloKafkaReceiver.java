package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.AloFactoryConfig;
import io.atleon.core.AloFlux;
import io.atleon.core.OrderManagingAcknowledgementOperator;
import io.atleon.util.ConfigLoading;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A reactive Kafka receiver with at-least-once semantics for consuming records from topics of a
 * Kafka cluster.
 * <p>
 * Each subscription to returned {@link AloFlux}s is backed by a Kafka
 * {@link org.apache.kafka.clients.consumer.Consumer Consumer}. When a subscription is terminated
 * for any reason, the Consumer is closed.
 * <p>
 * Offsets are committed periodically based on the Records that have been acknowledged
 * downstream.
 * <p>
 * Emitted records may be acknowledged in any order. In order to maintain at-least-once
 * semantics, no offset for an acknowledged record is committed unless all emitted records with
 * lesser offsets are acknowledged first. This ordering is maintained per partition. For
 * example, given a topic T with two partitions 0 and 1, we have records A, B, C respectively
 * on T-0 and D, E, F respectively on T-1. The records are emitted in order T-0-A, T-1-D,
 * T-0-B, T-1-E, T-0-C, T-1-F. At commit time, records T-0-B, T-0-C, T-1-D, and T-1-E have been
 * acknowledged. Therefore, no further offset would be committed for T-0, since T-0-A has not
 * yet been acknowledged, and the offset for T-1-E would be committed since, T-1-D and T-1-E
 * have been acknowledged.
 * <p>
 * Note that {@link io.atleon.core.AloDecorator AloDecorators} applied via
 * {@link io.atleon.core.AloDecoratorConfig#ALO_DECORATOR_TYPES_CONFIG} must be
 * implementations of {@link AloKafkaConsumerRecordDecorator}.
 *
 * @param <K> inbound record key type
 * @param <V> inbound record value type
 */
public class AloKafkaReceiver<K, V> {

    /**
     * Prefix used on all AloKafkaReceiver-specific configurations
     */
    public static final String CONFIG_PREFIX = "kafka.receiver.";

    /**
     * Subscribers may want to block request Threads on assignment of partitions AND subsequent
     * fetching/updating of offset positions on those partitions such that all imminently
     * produced Records to the subscribed Topics will be received by the associated Consumer
     * Group. This can help avoid timing problems, particularly with tests, and avoids having
     * to use `auto.offset.reset = "earliest"` to guarantee receipt of Records immediately
     * produced by the request Thread (directly or indirectly)
     */
    public static final String BLOCK_REQUEST_ON_PARTITION_POSITIONS_CONFIG = CONFIG_PREFIX + "block.request.on.partition.positions";

    /**
     * Controls the number of outstanding unacknowledged Records emitted per subscription. This is
     * helpful in controlling the number of data elements allowed in memory, particularly when
     * stream processes use any sort of buffering, windowing, or reduction operation(s).
     */
    public static final String MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG = CONFIG_PREFIX + "max.in.flight.per.subscription";

    /**
     * It may be desirable to have client IDs be incremented per subscription. This can remedy
     * conflicts with external resource registration (i.e. JMX) if the same client ID is expected
     * to have concurrent subscriptions
     */
    public static final String AUTO_INCREMENT_CLIENT_ID_CONFIG = CONFIG_PREFIX + "auto.increment.client.id";

    /**
     * Controls timeouts of polls to Kafka. This config can be increased if a Kafka cluster is
     * slow to respond. Specified as ISO-8601 Duration, e.g. PT10S
     */
    public static final String POLL_TIMEOUT_CONFIG = CONFIG_PREFIX + "poll.timeout";

    /**
     * Interval with which to commit offsets associated with acknowledged Records. Specified as
     * ISO-8601 Duration, e.g. PT5S
     */
    public static final String COMMIT_INTERVAL_CONFIG = CONFIG_PREFIX + "commit.interval";

    /**
     * Committing Offsets can fail for retriable reasons. This config can be increased if
     * failing to commit offsets is found to be particularly frequent
     */
    public static final String MAX_COMMIT_ATTEMPTS_CONFIG = CONFIG_PREFIX + "max.commit.attempts";

    /**
     * Closing the underlying Kafka Consumer is a fallible process. In order to not infinitely
     * deadlock a Consumer during this process (which can lead to non-consumption of assigned
     * partitions), we use a default equal to what's used in KafkaConsumer::close
     */
    public static final String CLOSE_TIMEOUT_CONFIG = CONFIG_PREFIX + "close.timeout";

    private static final boolean DEFAULT_BLOCK_REQUEST_ON_PARTITION_POSITIONS = false;

    private static final long DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION = 4096;

    private static final boolean DEFAULT_AUTO_INCREMENT_CLIENT_ID = false;

    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100L);

    private static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(5L);

    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 100;

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30L);

    private static final Logger LOGGER = LoggerFactory.getLogger(AloKafkaReceiver.class);

    private static final Map<String, AtomicLong> COUNTS_BY_CLIENT_ID = new ConcurrentHashMap<>();

    private final KafkaConfigSource configSource;

    private AloKafkaReceiver(KafkaConfigSource configSource) {
        this.configSource = configSource;
    }

    /**
     * Creates a new AloKafkaReceiver from the provided {@link KafkaConfigSource}
     *
     * @param configSource The reactive source of Kafka Receiver properties
     * @param <K>          The types of keys contained in received records
     * @param <V>          The types of values contained in received records
     * @return A new AloKafkaReceiver
     */
    public static <K, V> AloKafkaReceiver<K, V> from(KafkaConfigSource configSource) {
        return new AloKafkaReceiver<>(configSource);
    }

    /**
     * Creates a new AloKafkaReceiver from the provided {@link KafkaConfigSource} where only the
     * types of values contained in received Records is relevant. This is mainly useful if record
     * key values are not relevant or meaningfully used.
     *
     * @param configSource The reactive source of Kafka Receiver properties
     * @param <V>          The types of values contained in received records
     * @return A new AloKafkaReceiver
     */
    public static <V> AloKafkaReceiver<Object, V> forValues(KafkaConfigSource configSource) {
        return new AloKafkaReceiver<>(configSource);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing values extracted from Kafka
     * {@link ConsumerRecord}s wrapped as an {@link AloFlux}.
     * <p>
     * Note that the Reactive Streams specification does not allow emission of null items.
     * Therefore, received records that have null values are filtered and immediately acknowledged.
     *
     * @param topic The topic to subscribe to
     * @return A Publisher of Alo items referencing values extracted from Kafka ConsumerRecords
     */
    public AloFlux<V> receiveAloValues(String topic) {
        return receiveAloValues(Collections.singletonList(topic));
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing values extracted from Kafka
     * {@link ConsumerRecord}s wrapped as an {@link AloFlux}.
     *
     * @param topics The collection of topics to subscribe to
     * @return A Publisher of Alo items referencing values extracted from Kafka ConsumerRecords
     */
    public AloFlux<V> receiveAloValues(Collection<String> topics) {
        return receiveAloRecords(topics)
            .mapNotNull(ConsumerRecord::value);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing values extracted from Kafka
     * {@link ConsumerRecord}s wrapped as an {@link AloFlux}.
     *
     * @param topic The topic to subscribe to
     * @return A Publisher of Alo items referencing Kafka ConsumerRecords
     */
    public AloFlux<ConsumerRecord<K, V>> receiveAloRecords(String topic) {
        return receiveAloRecords(Collections.singletonList(topic));
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing values extracted from Kafka
     * {@link ConsumerRecord}s wrapped as an {@link AloFlux}.
     *
     * @param topics The collection of topics to subscribe to
     * @return A Publisher of Alo items referencing Kafka ConsumerRecords
     */
    public AloFlux<ConsumerRecord<K, V>> receiveAloRecords(Collection<String> topics) {
        return configSource.create()
            .flatMapMany(config -> receiveRecords(config, topics))
            .as(AloFlux::wrap);
    }

    private Flux<Alo<ConsumerRecord<K, V>>> receiveRecords(Map<String, Object> config, Collection<String> topics) {
        Map<String, Object> options = new HashMap<>();
        loadInto(options, config, BLOCK_REQUEST_ON_PARTITION_POSITIONS_CONFIG, DEFAULT_BLOCK_REQUEST_ON_PARTITION_POSITIONS);
        loadInto(options, config, MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG, DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION);
        loadInto(options, config, AUTO_INCREMENT_CLIENT_ID_CONFIG, DEFAULT_AUTO_INCREMENT_CLIENT_ID);
        loadInto(options, config, POLL_TIMEOUT_CONFIG, DEFAULT_POLL_TIMEOUT);
        loadInto(options, config, COMMIT_INTERVAL_CONFIG, DEFAULT_COMMIT_INTERVAL);
        loadInto(options, config, MAX_COMMIT_ATTEMPTS_CONFIG, DEFAULT_MAX_COMMIT_ATTEMPTS);
        loadInto(options, config, CLOSE_TIMEOUT_CONFIG, DEFAULT_CLOSE_TIMEOUT);

        AloFactory<ConsumerRecord<K, V>> aloFactory = AloFactoryConfig.loadDecorated(config, AloKafkaConsumerRecordDecorator.class);
        long maxInFlightPerSubscription = ConfigLoading.loadLongOrThrow(options, MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG);

        // 1) Create Consumer config from remaining configs that are NOT options (help avoid WARNs)
        // 2) Note Client ID and, if enabled, increment client ID in consumer config
        Map<String, Object> consumerConfig = new HashMap<>(config);
        options.keySet().forEach(consumerConfig::remove);
        String clientId = ConfigLoading.loadStringOrThrow(config, CommonClientConfigs.CLIENT_ID_CONFIG);
        if (ConfigLoading.loadBooleanOrThrow(options, AUTO_INCREMENT_CLIENT_ID_CONFIG)) {
            consumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-" + nextClientIdCount(clientId));
        }

        // Create Future that allows blocking on partition assignment and positioning
        CompletableFuture<Collection<ReceiverPartition>> assignedPartitions =
            ConfigLoading.loadBooleanOrThrow(options, BLOCK_REQUEST_ON_PARTITION_POSITIONS_CONFIG) ?
                new CompletableFuture<>() : CompletableFuture.completedFuture(Collections.emptyList());
        Future<?> positions = assignedPartitions.thenAccept(partitions -> partitions.forEach(ReceiverPartition::position));

        ReceiverOptions<K, V> receiverOptions = ReceiverOptions.<K, V>create(consumerConfig)
            .subscription(topics)
            .pollTimeout(ConfigLoading.loadDurationOrThrow(options, POLL_TIMEOUT_CONFIG))
            .commitInterval(ConfigLoading.loadDurationOrThrow(options, COMMIT_INTERVAL_CONFIG))
            .maxCommitAttempts(ConfigLoading.loadIntOrThrow(options, MAX_COMMIT_ATTEMPTS_CONFIG))
            .closeTimeout(ConfigLoading.loadDurationOrThrow(options, CLOSE_TIMEOUT_CONFIG))
            .addAssignListener(assignedPartitions::complete);

        return KafkaReceiver.create(receiverOptions).receive()
            .transform(records -> positions.isDone() ? records : records.mergeWith(blockRequestOn(positions)))
            .transform(records -> createAloRecords(records, aloFactory, maxInFlightPerSubscription));
    }

    private Flux<Alo<ConsumerRecord<K, V>>> createAloRecords(
        Flux<ReceiverRecord<K, V>> records,
        AloFactory<ConsumerRecord<K, V>> aloFactory,
        long maxInFlightPerSubscription
    ) {
        Sinks.Empty<Alo<ConsumerRecord<K, V>>> sink = Sinks.empty();
        return records.map(record -> aloFactory.create(record, record.receiverOffset()::acknowledge, sink::tryEmitError))
            .mergeWith(sink.asMono())
            .transform(aloRecords -> new OrderManagingAcknowledgementOperator<>(
                aloRecords, ConsumerRecordExtraction::topicPartition, maxInFlightPerSubscription));
    }

    private static long nextClientIdCount(String clientId) {
        return COUNTS_BY_CLIENT_ID.computeIfAbsent(clientId, key -> new AtomicLong()).incrementAndGet();
    }

    private static void loadInto(Map<String, Object> destination, Map<String, Object> source, String key, Object def) {
        destination.put(key, source.getOrDefault(key, def));
    }

    private static <T> Mono<T> blockRequestOn(Future<?> future) {
        return Mono.<T>empty().doOnRequest(requested -> {
            try {
                future.get();
            } catch (Exception e) {
                LOGGER.error("Failed to block Request Thread on Future", e);
            }
        });
    }
}
