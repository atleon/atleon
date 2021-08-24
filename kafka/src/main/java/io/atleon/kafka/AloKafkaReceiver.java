package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import io.atleon.core.ConfigLoading;
import io.atleon.core.Defaults;
import io.atleon.core.Instantiation;
import io.atleon.core.OrderManagingAcknowledgementOperator;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
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
 *
 * @param <K> incoming record key type
 * @param <V> incoming record value type
 */
public class AloKafkaReceiver<K, V> {

    /**
     * Prefix used on all AloKafkaReceiver-specific configurations
     */
    public static final String CONFIG_PREFIX = "atleon.kafka.receiver.";

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
     * The fully qualified class name of an implementation of {@link AloConsumerRecordFactory} used
     * to create implementations of {@link Alo} to be emitted to Subscribers
     */
    public static final String ALO_FACTORY_CONFIG = CONFIG_PREFIX + "alo.factory";

    /**
     * Controls the number of outstanding unacknowledged Records emitted per subscription. This is
     * helpful in controlling the number of data elements allowed in memory, particularly when
     * stream processes use any sort of buffering, windowing, or reduction operation(s).
     */
    public static final String MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG = CONFIG_PREFIX + "max.in.flight.per.subscription";

    /**
     * Closing the underlying Kafka Consumer is a fallible process. In order to not infinitely
     * deadlock a Consumer during this process (which can lead to non-consumption of assigned
     * partitions), we use a default equal to what's used in KafkaConsumer::close
     */
    public static final String CLOSE_TIMEOUT_CONFIG = CONFIG_PREFIX + "close.timeout";

    private static final boolean DEFAULT_BLOCK_REQUEST_ON_PARTITION_POSITIONS = false;

    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100L);

    private static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(5L);

    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 100;

    private static final long DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION = 4096;

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30L);

    private static final Logger LOGGER = LoggerFactory.getLogger(AloKafkaReceiver.class);

    private static final Map<String, AtomicLong> COUNTS_BY_CLIENT_ID = new ConcurrentHashMap<>();

    private static final Map<String, Scheduler> SCHEDULERS_BY_CLIENT_ID = new ConcurrentHashMap<>();

    private final KafkaConfigFactory configFactory;

    private AloKafkaReceiver(KafkaConfigFactory configFactory) {
        this.configFactory = configFactory;
    }

    public static <K, V> AloKafkaReceiver<K, V> from(KafkaConfigFactory configFactory) {
        return new AloKafkaReceiver<>(configFactory);
    }

    public static <V> AloKafkaReceiver<Object, V> forValues(KafkaConfigFactory configFactory) {
        return new AloKafkaReceiver<>(configFactory);
    }

    public AloFlux<V> receiveAloValues(Collection<String> topics) {
        return receiveAlo(topics)
            .filter(record -> record.value() != null)
            .map(ConsumerRecord::value);
    }

    public AloFlux<ConsumerRecord<K, V>> receiveAlo(Collection<String> topics) {
        return configFactory.create()
            .flatMapMany(config -> receiveRecords(config, topics))
            .as(AloFlux::wrap);
    }

    private Flux<Alo<ConsumerRecord<K, V>>> receiveRecords(Map<String, Object> config, Collection<String> topics) {
        Map<String, Object> options = new HashMap<>();
        options.put(BLOCK_REQUEST_ON_PARTITION_POSITIONS_CONFIG, config.getOrDefault(BLOCK_REQUEST_ON_PARTITION_POSITIONS_CONFIG, DEFAULT_BLOCK_REQUEST_ON_PARTITION_POSITIONS));
        options.put(POLL_TIMEOUT_CONFIG, config.getOrDefault(POLL_TIMEOUT_CONFIG, DEFAULT_POLL_TIMEOUT));
        options.put(COMMIT_INTERVAL_CONFIG, config.getOrDefault(COMMIT_INTERVAL_CONFIG, DEFAULT_COMMIT_INTERVAL));
        options.put(MAX_COMMIT_ATTEMPTS_CONFIG, config.getOrDefault(MAX_COMMIT_ATTEMPTS_CONFIG, DEFAULT_MAX_COMMIT_ATTEMPTS));
        options.put(MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG, config.getOrDefault(MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG, DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION));
        options.put(CLOSE_TIMEOUT_CONFIG, config.getOrDefault(CLOSE_TIMEOUT_CONFIG, DEFAULT_CLOSE_TIMEOUT));

        AloConsumerRecordFactory<K, V> aloFactory = createAloFactory(config);
        long maxInFlightPerSubscription = ConfigLoading.loadOrThrow(options, MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG, Long::valueOf);

        // 1) Create Consumer config from remaining configs that are NOT options (help avoid WARNs)
        // 2) Note Client ID and make unique to avoid conflicting registration with resources (i.e. JMX)
        Map<String, Object> consumerConfig = new HashMap<>(config);
        options.keySet().forEach(consumerConfig::remove);
        String clientId = ConfigLoading.loadOrThrow(config, CommonClientConfigs.CLIENT_ID_CONFIG, Object::toString);
        consumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-" + nextClientIdCount(clientId));

        ReceiverOptions<K, V> receiverOptions = ReceiverOptions.create(consumerConfig);
        receiverOptions.subscription(topics);
        receiverOptions.pollTimeout(ConfigLoading.loadOrThrow(options, POLL_TIMEOUT_CONFIG, Duration::parse));
        receiverOptions.commitInterval(ConfigLoading.loadOrThrow(options, COMMIT_INTERVAL_CONFIG, Duration::parse));
        receiverOptions.maxCommitAttempts(ConfigLoading.loadOrThrow(options, MAX_COMMIT_ATTEMPTS_CONFIG, Integer::valueOf));
        receiverOptions.closeTimeout(ConfigLoading.loadOrThrow(options, CLOSE_TIMEOUT_CONFIG, Duration::parse));
        receiverOptions.schedulerSupplier(() -> schedulerForClient(clientId));

        // Create Future that allows blocking on partition assignment and positioning
        CompletableFuture<Collection<ReceiverPartition>> assignedPartitions =
            ConfigLoading.loadOrThrow(options, BLOCK_REQUEST_ON_PARTITION_POSITIONS_CONFIG, Boolean::valueOf) ?
                new CompletableFuture<>() : CompletableFuture.completedFuture(Collections.emptyList());
        Future<?> assignedPartitionPositions = assignedPartitions.thenAccept(partitions -> partitions.forEach(ReceiverPartition::position));
        receiverOptions.addAssignListener(assignedPartitions::complete);

        return KafkaReceiver.create(receiverOptions).receive()
            .transform(records -> assignedPartitionPositions.isDone() ? records : records.mergeWith(blockRequestOn(assignedPartitionPositions)))
            .transformDeferred(records -> createAloRecords(records, aloFactory, maxInFlightPerSubscription));
    }

    private Flux<Alo<ConsumerRecord<K, V>>> createAloRecords(
        Flux<ReceiverRecord<K, V>> records,
        AloConsumerRecordFactory<K, V> aloFactory,
        long maxInFlightPerSubscription) {
        Sinks.Empty<Alo<ConsumerRecord<K, V>>> sink = Sinks.empty();
        return records.map(record -> aloFactory.create(record, record.receiverOffset()::acknowledge, sink::tryEmitError))
            .mergeWith(sink.asMono())
            .transform(aloRecords -> new OrderManagingAcknowledgementOperator<>(
                aloRecords, ConsumerRecordExtraction::extractTopicPartition, maxInFlightPerSubscription));
    }

    private static long nextClientIdCount(String clientId) {
        return COUNTS_BY_CLIENT_ID.computeIfAbsent(clientId, key -> new AtomicLong()).incrementAndGet();
    }

    private static Scheduler schedulerForClient(String clientId) {
        return SCHEDULERS_BY_CLIENT_ID.computeIfAbsent(clientId, AloKafkaReceiver::newSchedulerForClient);
    }

    private static Scheduler newSchedulerForClient(String clientId) {
        String schedulerName = AloKafkaReceiver.class.getSimpleName() + "-" + clientId;
        return Schedulers.newBoundedElastic(Defaults.THREAD_CAP, Integer.MAX_VALUE, schedulerName);
    }

    private static <K, V> AloConsumerRecordFactory<K, V> createAloFactory(Map<String, Object> config) {
        AloConsumerRecordFactory<K, V> aloConsumerRecordFactory =
            ConfigLoading.load(config, ALO_FACTORY_CONFIG, Instantiation::<AloConsumerRecordFactory<K, V>>one)
                .orElseGet(DefaultAloConsumerRecordFactory::new);
        aloConsumerRecordFactory.configure(config);
        return aloConsumerRecordFactory;
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
