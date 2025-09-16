package io.atleon.kafka;

import io.atleon.core.AcknowledgementQueueMode;
import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.AloFactoryConfig;
import io.atleon.core.AloFlux;
import io.atleon.core.AloQueueListenerConfig;
import io.atleon.core.AloSignalListenerFactory;
import io.atleon.core.AloSignalListenerFactoryConfig;
import io.atleon.util.Defaults;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

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
 * {@link io.atleon.core.AloDecoratorConfig#DECORATOR_TYPES_CONFIG} must be
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
     * Configures the operating mode of the Acknowledgement Queue used to maintain commitment order
     * of consumer offsets. By default, the mode is STRICT, where any given record's offset is not
     * committed until all records received before it have been committed, and each record's offset
     * is explicitly committed. In COMPACT mode, it is still the case that no record's offset is
     * committed until all records received before it have been committed, but commitment of a
     * given record's offset may be skipped if there are already multiple sequential records after
     * it that are already ready for commitment.
     */
    public static final String ACKNOWLEDGEMENT_QUEUE_MODE_CONFIG = CONFIG_PREFIX + "acknowledgement.queue.mode";

    /**
     * Configures the type of strategy to use for polling. Some simple types are available that
     * target static constructors, like {@value #POLL_STRATEGY_FACTORY_TYPE_NATURAL},
     * {@value #POLL_STRATEGY_FACTORY_TYPE_BINARY_STRIDES}, and
     * {@value #POLL_STRATEGY_FACTORY_TYPE_GREATEST_BATCH_LAG}. Any other non-predefined value is
     * treated as a qualified class name of an implementation of {@link PollStrategyFactory}.
     *
     * @see PollStrategyFactory
     */
    public static final String POLL_STRATEGY_FACTORY_CONFIG = CONFIG_PREFIX + "poll.strategy.factory";

    public static final String POLL_STRATEGY_FACTORY_TYPE_NATURAL = "natural";

    public static final String POLL_STRATEGY_FACTORY_TYPE_BINARY_STRIDES = "binary-strides";

    public static final String POLL_STRATEGY_FACTORY_TYPE_GREATEST_BATCH_LAG = "greatest-batch-lag";

    /**
     * Configures the behavior of negatively acknowledging received records. Some simple types are
     * available, including {@value #NACKNOWLEDGER_TYPE_EMIT}, where the associated error is
     * emitted in to the pipeline. Any other non-predefined value is treated as a qualified class
     * name of an implementation of {@link NacknowledgerFactory} which allows more fine-grained
     * control over what happens when an SQS Message is negatively acknowledged. Defaults to
     * "emit".
     */
    public static final String NACKNOWLEDGER_TYPE_CONFIG = CONFIG_PREFIX + "nacknowledger.type";

    public static final String NACKNOWLEDGER_TYPE_EMIT = "emit";

    /**
     * When negative acknowledgement results in emitting the corresponding error, this configures
     * the timeout on successfully emitting that error.
     */
    public static final String ERROR_EMISSION_TIMEOUT_CONFIG = CONFIG_PREFIX + "error.emission.timeout";

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
     * Directly controls the maximum number records that may be prefetched in memory for subsequent
     * emission (which is independent of the number of in-flight records per subscription). The
     * maximum number of prefetched records is calculated based on the product of this and Kafka's
     * native {@link org.apache.kafka.clients.consumer.ConsumerConfig#MAX_POLL_RECORDS_CONFIG}.
     */
    public static final String FULL_POLL_RECORDS_PREFETCH_CONFIG = CONFIG_PREFIX + "full.poll.records.prefetch";

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

    /**
     * When rebalances occur, this configures the maximum duration that will be awaited for
     * in-flight records to be acknowledged before allowing the rebalance to complete.
     *
     * @deprecated Use {@link #REVOCATION_GRACE_PERIOD_CONFIG}
     */
    @Deprecated
    public static final String MAX_DELAY_REBALANCE_CONFIG = CONFIG_PREFIX + "max.delay.rebalance";

    /**
     * Upon rebalancing, this configuration controls how long to wait for in-flight records from
     * any given revoked partition to be acknowledged before letting the rebalance continue.
     */
    public static final String REVOCATION_GRACE_PERIOD_CONFIG = CONFIG_PREFIX + "revocation.grace.period";

    /**
     * This is a temporary configuration to enable and test usage of re-optimized Kafka receiver.
     * Set to "OPTIMIZED" to enable.
     */
    @Deprecated
    public static final String RECEPTION_TYPE_CONFIG = CONFIG_PREFIX + "reception.type";

    private static final AcknowledgementQueueMode DEFAULT_ACKNOWLEDGEMENT_QUEUE_MODE = AcknowledgementQueueMode.STRICT;

    private static final long DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION = 4096;

    private static final boolean DEFAULT_AUTO_INCREMENT_CLIENT_ID = false;

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30L);

    private static final Map<String, AtomicLong> COUNTS_BY_ID = new ConcurrentHashMap<>();

    private final KafkaConfigSource configSource;

    private AloKafkaReceiver(KafkaConfigSource configSource) {
        this.configSource = configSource;
    }

    /**
     * Alias for {@link #create(KafkaConfigSource)}. Will be deprecated in future release.
     */
    public static <K, V> AloKafkaReceiver<K, V> from(KafkaConfigSource configSource) {
        return create(configSource);
    }

    /**
     * Creates a new AloKafkaReceiver from the provided {@link KafkaConfigSource}
     *
     * @param configSource The reactive source of Kafka Receiver properties
     * @param <K>          The types of keys contained in received records
     * @param <V>          The types of values contained in received records
     * @return A new AloKafkaReceiver
     */
    public static <K, V> AloKafkaReceiver<K, V> create(KafkaConfigSource configSource) {
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
     * @deprecated Use {@link AloKafkaReceiver#create(KafkaConfigSource)}
     */
    @Deprecated
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
     * @param topicsPattern The {@link Pattern} of topics to subscribe to
     * @return A Publisher of Alo items referencing values extracted from Kafka ConsumerRecords
     */
    public AloFlux<V> receiveAloValues(Pattern topicsPattern) {
        return receiveAloRecords(topicsPattern)
            .mapNotNull(ConsumerRecord::value);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing Kafka {@link ConsumerRecord}s wrapped
     * as an {@link AloFlux}.
     *
     * @param topic The topic to subscribe to
     * @return A Publisher of Alo items referencing Kafka ConsumerRecords
     */
    public AloFlux<ConsumerRecord<K, V>> receiveAloRecords(String topic) {
        return receiveAloRecords(Collections.singletonList(topic));
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing Kafka {@link ConsumerRecord}s wrapped
     * as an {@link AloFlux}.
     *
     * @param topics The collection of topics to subscribe to
     * @return A Publisher of Alo items referencing Kafka ConsumerRecords
     */
    public AloFlux<ConsumerRecord<K, V>> receiveAloRecords(Collection<String> topics) {
        ReceptionFactory<K, V> receptionFactory = ReceptionFactory.topics(topics);
        return configSource.create().flatMapMany(receptionFactory::receive).as(AloFlux::wrap);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing Kafka {@link ConsumerRecord}s wrapped
     * as an {@link AloFlux}.
     *
     * @param topicsPattern The {@link Pattern} of topics to subscribe to
     * @return A Publisher of Alo items referencing Kafka ConsumerRecords
     */
    public AloFlux<ConsumerRecord<K, V>> receiveAloRecords(Pattern topicsPattern) {
        ReceptionFactory<K, V> receptionFactory = ReceptionFactory.topicsPattern(topicsPattern);
        return configSource.create().flatMapMany(receptionFactory::receive).as(AloFlux::wrap);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing Kafka {@link ConsumerRecord}s wrapped
     * as an {@link AloFlux}. This is a special mode of reception where received records are
     * prioritized based on the provided {@link ReceptionPrioritization}. The prioritization
     * is used to determine how many Kafka Consumer instances will back the created {@link AloFlux},
     * and subsequently how records from each of those consumers will be prioritized for emission.
     * This is accomplished by first listing the partitions that will be subscribed to, mapping
     * those to priority "levels", and then creating a Consumer for each level. In order for this
     * to work as intended, it should be the case that each priority "level"/grouping of partitions
     * will be mutually exclusively assigned to any given Consumer. In the case of consuming a
     * single topic and using the partition number as the "priority", this condition is satisfied
     * out of the box, since this method ensures there will be at least as many Consumers as there
     * are partitions, and therefore "levels". More sophisticated use cases may require a
     * special/custom {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor} to ensure
     * that partitions with different "levels" are not assigned to the same Consumer instance. In
     * another case, it may be desirable to configure a partition assignor
     * (like {@link SingleMachinePartitionAssignor}) that ensures all partitions are assigned to
     * Consumer instances on a single machine, thus accomplishing a "global" prioritization of
     * processing.
     *
     * @param topic          The topic to consume from
     * @param prioritization Indicates the priority of consuming any given {@link TopicPartition}
     * @return A Publisher of Alo items referencing prioritized Kafka ConsumerRecords
     */
    public AloFlux<ConsumerRecord<K, V>>
    receivePrioritizedAloRecords(String topic, ReceptionPrioritization prioritization) {
        return receivePrioritizedAloRecords(Collections.singleton(topic), prioritization, Defaults.PREFETCH);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing Kafka {@link ConsumerRecord}s wrapped
     * as an {@link AloFlux}. This is a special mode of reception where received records are
     * prioritized based on the provided {@link ReceptionPrioritization}. The prioritization
     * is used to determine how many Kafka Consumer instances will back the created {@link AloFlux},
     * and subsequently how records from each of those consumers will be prioritized for emission.
     * This is accomplished by first listing the partitions that will be subscribed to, mapping
     * those to priority "levels", and then creating a Consumer for each level. In order for this
     * to work as intended, it should be the case that each priority "level"/grouping of partitions
     * will be mutually exclusively assigned to any given Consumer. In the case of consuming a
     * single topic and using the partition number as the "priority", this condition is satisfied
     * out of the box, since this method ensures there will be at least as many Consumers as there
     * are partitions, and therefore "levels". More sophisticated use cases may require a
     * special/custom {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor} to ensure
     * that partitions with different "levels" are not assigned to the same Consumer instance. In
     * another case, it may be desirable to configure a partition assignor
     * (like {@link SingleMachinePartitionAssignor}) that ensures all partitions are assigned to
     * Consumer instances on a single machine, thus accomplishing a "global" prioritization of
     * processing.
     *
     * @param topics         The topic to consume from
     * @param prioritization Indicates the priority of consuming any given {@link TopicPartition}
     * @param prefetch       The number of elements to prefetch from each merged sequence
     * @return A Publisher of Alo items referencing prioritized Kafka ConsumerRecords
     */
    public AloFlux<ConsumerRecord<K, V>>
    receivePrioritizedAloRecords(Collection<String> topics, ReceptionPrioritization prioritization, int prefetch) {
        ReceptionFactory<K, V> receptionFactory = ReceptionFactory.topics(topics);
        return configSource.create()
            .flatMap(it -> createOrderedConfigs(it, topics, prioritization))
            .map(receptionFactory::receive)
            .flatMapMany(it -> Flux.mergePriority(prefetch, toComparator(prioritization), it))
            .as(AloFlux::wrap);
    }

    private static Mono<List<KafkaConfig>>
    createOrderedConfigs(KafkaConfig config, Collection<String> topics, ReceptionPrioritization prioritization) {
        return listTopicPartitions(config, topics)
            .map(it -> prioritize(prioritization, it))
            .distinct()
            .sort(Comparator.naturalOrder())
            .map(it -> config.withClientIdSuffix("-", "p" + it))
            .collectList();
    }

    private static Flux<TopicPartition> listTopicPartitions(KafkaConfig config, Collection<String> topics) {
        return Flux.using(
            () -> ReactiveAdmin.create(config.nativeProperties()),
            it -> it.listTopicPartitions(topics),
            ReactiveAdmin::close);
    }

    private static int prioritize(ReceptionPrioritization prioritization, TopicPartition topicPartition) {
        int priority = prioritization.prioritize(topicPartition);
        if (priority < 0) {
            throw new IllegalStateException("Priority must be non-negative for topicPartition=" + topicPartition);
        }
        return priority;
    }

    private static <K, V> Comparator<Alo<ConsumerRecord<K, V>>> toComparator(ReceptionPrioritization prioritization) {
        return Comparator.comparing(alo ->
            prioritization.prioritize(ConsumerRecordExtraction.topicPartition(alo.get())));
    }

    private interface ReceptionInvocation<K, V> {

        Flux<KafkaReceiverRecord<K, V>> invoke(KafkaReceiver<K, V> receiver);
    }

    private static final class ReceptionFactory<K, V> {

        private final ReceptionInvocation<K, V> receptionInvocation;

        private final LegacyReceiveResources.OptionsInitializer<K, V> optionsInitializer;

        private final Map<Object, ConsumerMutexEnforcer> consumerMutexEnforcers = new ConcurrentHashMap<>();

        private ReceptionFactory(
            ReceptionInvocation<K, V> receptionInvocation,
            LegacyReceiveResources.OptionsInitializer<K, V> optionsInitializer
        ) {
            this.receptionInvocation = receptionInvocation;
            this.optionsInitializer = optionsInitializer;
        }

        public static <K, V> ReceptionFactory<K, V> topics(Collection<String> topics) {
            return new ReceptionFactory<>(
                receiver -> receiver.receiveManual(topics),
                config -> ReceiverOptions.<K, V>create(config).subscription(topics));
        }

        public static <K, V> ReceptionFactory<K, V> topicsPattern(Pattern topicsPattern) {
            return new ReceptionFactory<>(
                receiver -> receiver.receiveManual(topicsPattern),
                config -> ReceiverOptions.<K, V>create(config).subscription(topicsPattern));
        }

        @SuppressWarnings("unchecked")
        public Flux<Alo<ConsumerRecord<K, V>>>[] receive(List<KafkaConfig> orderedConfigs) {
            // Config index used as config key, since mutex should be maintained per reception lifecycle
            return IntStream.range(0, orderedConfigs.size())
                .mapToObj(it -> receive(it, orderedConfigs.get(it)))
                .toArray(Flux[]::new);
        }

        public Flux<Alo<ConsumerRecord<K, V>>> receive(KafkaConfig config) {
            // Constant used as config key, since mutex should be maintained for single reception lifecycle
            return receive(-1, config);
        }

        private Flux<Alo<ConsumerRecord<K, V>>> receive(Object configKey, KafkaConfig config) {
            if (config.loadString(RECEPTION_TYPE_CONFIG).orElse("LEGACY").equalsIgnoreCase("OPTIMIZED")) {
                return new ReceiveResources<K, V>(config).receive(receptionInvocation);
            } else {
                ConsumerMutexEnforcer consumerMutexEnforcer =
                    consumerMutexEnforcers.computeIfAbsent(configKey, __ -> new ConsumerMutexEnforcer());
                return new LegacyReceiveResources<K, V>(config).receive(optionsInitializer, consumerMutexEnforcer);
            }
        }
    }

    private static final class ReceiveResources<K, V> {

        private final KafkaConfig config;

        private final PollStrategyFactory pollStrategyFactory;

        private final AcknowledgementQueueMode acknowledgementQueueMode;

        private final NacknowledgerFactory<K, V> nacknowledgerFactory;

        private final AloFactory<ConsumerRecord<K, V>> aloFactory;

        public ReceiveResources(KafkaConfig config) {
            this.config = config;
            this.pollStrategyFactory = createPollStrategyFactory(config);
            this.acknowledgementQueueMode = loadAcknowledgementQueueMode(config);
            this.nacknowledgerFactory = createNacknowledgerFactory(config);
            this.aloFactory = loadAloFactory(config);
        }

        public Flux<Alo<ConsumerRecord<K, V>>> receive(ReceptionInvocation<K, V> receptionInvocation) {
            KafkaReceiverOptions<K, V> options = newReceiverOptions();
            return receptionInvocation.invoke(KafkaReceiver.create(options))
                .map(this::toAloConsumerRecord)
                .transform(this::applySignalListenerFactories);
        }

        private KafkaReceiverOptions<K, V> newReceiverOptions() {
            KafkaReceiverOptions<K, V> defaultOptions = KafkaReceiverOptions.defaultOptions();
            return KafkaReceiverOptions.<K, V>newBuilder()
                .consumerProperties(newConsumerConfig())
                .receptionListener(loadReceptionListener())
                .pollStrategyFactory(pollStrategyFactory)
                .fullPollRecordsPrefetch(config.loadInt(FULL_POLL_RECORDS_PREFETCH_CONFIG)
                    .orElse(defaultOptions.fullPollRecordsPrefetch()))
                .maxActiveInFlight(config.loadLong(MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG)
                    .orElse(DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION))
                .pollTimeout(config.loadDuration(POLL_TIMEOUT_CONFIG).orElse(defaultOptions.pollTimeout()))
                .acknowledgementQueueMode(acknowledgementQueueMode)
                .commitPeriod(config.loadDuration(COMMIT_INTERVAL_CONFIG).orElse(defaultOptions.commitPeriod()))
                .maxCommitAttempts(config.loadInt(MAX_COMMIT_ATTEMPTS_CONFIG).orElse(defaultOptions.maxCommitAttempts()))
                .revocationGracePeriod(loadRevocationGracePeriod().orElse(defaultOptions.revocationGracePeriod()))
                .closeTimeout(config.loadDuration(CLOSE_TIMEOUT_CONFIG).orElse(DEFAULT_CLOSE_TIMEOUT))
                .build();
        }

        private Map<String, Object> newConsumerConfig() {
            return config.modifyAndGetProperties(properties -> {
                // Remove any Atleon-specific config (helps avoid warning logs about unused config)
                properties.keySet().removeIf(key -> key.startsWith(CONFIG_PREFIX));

                // If enabled, increment Client ID
                if (config.loadBoolean(AUTO_INCREMENT_CLIENT_ID_CONFIG).orElse(DEFAULT_AUTO_INCREMENT_CLIENT_ID)) {
                    properties.computeIfPresent(CommonClientConfigs.CLIENT_ID_CONFIG, (__, id) -> incrementId(id.toString()));
                }
            });
        }

        private Optional<Duration> loadRevocationGracePeriod() {
            Optional<Duration> revocationGracePeriod = config.loadDuration(REVOCATION_GRACE_PERIOD_CONFIG);
            revocationGracePeriod = revocationGracePeriod.isPresent()
                ? revocationGracePeriod
                : config.loadDuration(MAX_DELAY_REBALANCE_CONFIG);
            return revocationGracePeriod.isPresent() || acknowledgementQueueMode == AcknowledgementQueueMode.STRICT
                ? revocationGracePeriod
                : Optional.of(Duration.ZERO); // By default, disable delay if acknowledgements may be skipped
        }

        private ReceptionListener loadReceptionListener() {
            Map<String, Object> listenerConfig = config.modifyAndGetProperties(properties -> {});
            return AloQueueListenerConfig.load(listenerConfig, AloKafkaQueueListener.class)
                .<ReceptionListener>map(AloQueueReceptionListener::new)
                .orElseGet(ReceptionListener::noOp);
        }

        private Alo<ConsumerRecord<K, V>> toAloConsumerRecord(KafkaReceiverRecord<K, V> receiverRecord) {
            Consumer<Throwable> nacknowledger =
                nacknowledgerFactory.create(receiverRecord.consumerRecord(), receiverRecord.nacknowledger());
            return aloFactory.create(receiverRecord.consumerRecord(), receiverRecord.acknowledger(), nacknowledger);
        }

        private Flux<Alo<ConsumerRecord<K, V>>> applySignalListenerFactories(Flux<Alo<ConsumerRecord<K, V>>> aloRecords) {
            Map<String, Object> factoryConfig = config.modifyAndGetProperties(properties -> {});
            List<AloSignalListenerFactory<ConsumerRecord<K, V>, ?>> factories =
                AloSignalListenerFactoryConfig.loadList(factoryConfig, AloKafkaConsumerRecordSignalListenerFactory.class);
            for (AloSignalListenerFactory<ConsumerRecord<K, V>, ?> factory : factories) {
                aloRecords = aloRecords.tap(factory);
            }
            return aloRecords;
        }

        private static PollStrategyFactory createPollStrategyFactory(KafkaConfig config) {
            Optional<PollStrategyFactory> pollStrategyFactory1 = config.loadInstanceWithPredefinedTypes(
                POLL_STRATEGY_FACTORY_CONFIG,
                PollStrategyFactory.class,
                ReceiveResources::newPredefinedPollStrategyFactory
            );
            return pollStrategyFactory1.orElseGet(PollStrategyFactory::natural);
        }

        private static Optional<PollStrategyFactory> newPredefinedPollStrategyFactory(String typeName) {
            if (typeName.equalsIgnoreCase(POLL_STRATEGY_FACTORY_TYPE_NATURAL)) {
                return Optional.of(PollStrategyFactory.natural());
            } else if (typeName.equalsIgnoreCase(POLL_STRATEGY_FACTORY_TYPE_BINARY_STRIDES)) {
                return Optional.of(PollStrategyFactory.binaryStrides());
            } else if (typeName.equalsIgnoreCase(POLL_STRATEGY_FACTORY_TYPE_GREATEST_BATCH_LAG)) {
                return Optional.of(PollStrategyFactory.greatestBatchLag());
            } else {
                return Optional.empty();
            }
        }

        private static <K, V> NacknowledgerFactory<K, V> createNacknowledgerFactory(KafkaConfig config) {
            Optional<NacknowledgerFactory<K, V>> nacknowledgerFactory =
                loadNacknowledgerFactory(config, NACKNOWLEDGER_TYPE_CONFIG, NacknowledgerFactory.class);
            return nacknowledgerFactory.orElseGet(NacknowledgerFactory.Emit::new);
        }

        private static <K, V, N extends NacknowledgerFactory<K, V>> Optional<NacknowledgerFactory<K, V>>
        loadNacknowledgerFactory(KafkaConfig config, String key, Class<N> type) {
            return config.loadConfiguredWithPredefinedTypes(key, type, ReceiveResources::newPredefinedNacknowledgerFactory);
        }

        private static <K, V> Optional<NacknowledgerFactory<K, V>> newPredefinedNacknowledgerFactory(String typeName) {
            if (typeName.equalsIgnoreCase(NACKNOWLEDGER_TYPE_EMIT)) {
                return Optional.of(new NacknowledgerFactory.Emit<>());
            } else {
                return Optional.empty();
            }
        }

        private static <K, V> AloFactory<ConsumerRecord<K, V>> loadAloFactory(KafkaConfig config) {
            Map<String, Object> factoryConfig = config.modifyAndGetProperties(properties -> {});
            return AloFactoryConfig.loadDecorated(factoryConfig, AloKafkaConsumerRecordDecorator.class);
        }

        private static AcknowledgementQueueMode loadAcknowledgementQueueMode(KafkaConfig config) {
            return config.loadEnum(ACKNOWLEDGEMENT_QUEUE_MODE_CONFIG, AcknowledgementQueueMode.class)
                .orElse(DEFAULT_ACKNOWLEDGEMENT_QUEUE_MODE);
        }

        private static String incrementId(String id) {
            return id + "-" + COUNTS_BY_ID.computeIfAbsent(id, __ -> new AtomicLong()).incrementAndGet();
        }
    }
}
