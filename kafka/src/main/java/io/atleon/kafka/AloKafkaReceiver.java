package io.atleon.kafka;

import io.atleon.core.AcknowledgementQueueMode;
import io.atleon.core.Alo;
import io.atleon.core.AloComponentExtractor;
import io.atleon.core.AloFactory;
import io.atleon.core.AloFactoryConfig;
import io.atleon.core.AloFlux;
import io.atleon.core.AloQueueListener;
import io.atleon.core.AloQueueListenerConfig;
import io.atleon.core.AloQueueingTransformer;
import io.atleon.core.AloSignalListenerFactory;
import io.atleon.core.AloSignalListenerFactoryConfig;
import io.atleon.core.ErrorEmitter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

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
     */
    public static final String MAX_DELAY_REBALANCE_CONFIG = CONFIG_PREFIX + "max.delay.rebalance";

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
        return receiveAloRecords(consumerConfig -> ReceiverOptions.<K, V>create(consumerConfig).subscription(topics));
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing values extracted from Kafka
     * {@link ConsumerRecord}s wrapped as an {@link AloFlux}.
     *
     * @param topicsPattern The {@link Pattern} of topics to subscribe to
     * @return A Publisher of Alo items referencing Kafka ConsumerRecords
     */
    public AloFlux<ConsumerRecord<K, V>> receiveAloRecords(Pattern topicsPattern) {
        return receiveAloRecords(consumerConfig -> ReceiverOptions.<K, V>create(consumerConfig).subscription(topicsPattern));
    }

    private AloFlux<ConsumerRecord<K, V>> receiveAloRecords(ReceiverOptionsInitializer<K, V> optionsInitializer) {
        ConsumerMutexEnforcer consumerMutexEnforcer = new ConsumerMutexEnforcer();
        return configSource.create()
            .map(ReceiveResources<K, V>::new)
            .flatMapMany(resources -> resources.receive(optionsInitializer, consumerMutexEnforcer))
            .as(AloFlux::wrap);
    }

    private interface ReceiverOptionsInitializer<K, V> {

        ReceiverOptions<K, V> initialize(Map<String, Object> consumerConfig);
    }

    private static final class ReceiveResources<K, V> {

        private final KafkaConfig config;

        private final NacknowledgerFactory<K, V> nacknowledgerFactory;

        private final AcknowledgementQueueMode acknowledgementQueueMode;

        public ReceiveResources(KafkaConfig config) {
            this.config = config;
            this.nacknowledgerFactory = createNacknowledgerFactory(config);
            this.acknowledgementQueueMode = loadAcknowledgementQueueMode(config);
        }

        public Flux<Alo<ConsumerRecord<K, V>>>
        receive(ReceiverOptionsInitializer<K, V> optionsInitializer, ConsumerMutexEnforcer consumerMutexEnforcer) {
            ErrorEmitter<Alo<ConsumerRecord<K, V>>> errorEmitter = newErrorEmitter();
            ReceiverOptions<K, V> options = newReceiverOptions(optionsInitializer);
            ConsumerMutexEnforcer.ProhibitableConsumerFactory consumerFactory = consumerMutexEnforcer.newConsumerFactory();
            return KafkaReceiver.create(consumerFactory, options).receive()
                .transform(newAloQueueingTransformer(errorEmitter::safelyEmit))
                .transform(errorEmitter::applyTo)
                .transform(this::applySignalListenerFactories)
                .doFinally(__ -> consumerFactory.prohibitFurtherConsumption(options.closeTimeout()));
        }

        private ErrorEmitter<Alo<ConsumerRecord<K, V>>> newErrorEmitter() {
            Duration timeout = config.loadDuration(ERROR_EMISSION_TIMEOUT_CONFIG).orElse(ErrorEmitter.DEFAULT_TIMEOUT);
            return ErrorEmitter.create(timeout);
        }

        private ReceiverOptions<K, V> newReceiverOptions(ReceiverOptionsInitializer<K, V> optionsInitializer) {
            ReceiverOptions<K, V> defaultOptions = optionsInitializer.initialize(newConsumerConfig());
            return defaultOptions
                .pollTimeout(config.loadDuration(POLL_TIMEOUT_CONFIG).orElse(defaultOptions.pollTimeout()))
                .commitInterval(config.loadDuration(COMMIT_INTERVAL_CONFIG).orElse(defaultOptions.commitInterval()))
                .maxCommitAttempts(config.loadInt(MAX_COMMIT_ATTEMPTS_CONFIG).orElse(defaultOptions.maxCommitAttempts()))
                .closeTimeout(config.loadDuration(CLOSE_TIMEOUT_CONFIG).orElse(DEFAULT_CLOSE_TIMEOUT))
                .maxDelayRebalance(loadMaxDelayRebalance().orElse(defaultOptions.maxDelayRebalance()));
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

        private Optional<Duration> loadMaxDelayRebalance() {
            Optional<Duration> maxDelayRebalance = config.loadDuration(MAX_DELAY_REBALANCE_CONFIG);
            return maxDelayRebalance.isPresent() || acknowledgementQueueMode == AcknowledgementQueueMode.STRICT
                ? maxDelayRebalance
                : Optional.of(Duration.ZERO); // By default, disable delay if acknowledgements may be skipped
        }

        private AloQueueingTransformer<ReceiverRecord<K, V>, ConsumerRecord<K, V>>
        newAloQueueingTransformer(Consumer<Throwable> errorEmitter) {
            return AloQueueingTransformer.create(newComponentExtractor(errorEmitter))
                .withGroupExtractor(record -> record.receiverOffset().topicPartition())
                .withQueueMode(acknowledgementQueueMode)
                .withListener(loadQueueListener())
                .withFactory(loadAloFactory())
                .withMaxInFlight(loadMaxInFlightPerSubscription());
        }

        private AloComponentExtractor<ReceiverRecord<K, V>, ConsumerRecord<K, V>>
        newComponentExtractor(Consumer<Throwable> errorEmitter) {
            return AloComponentExtractor.composed(
                record -> record.receiverOffset()::acknowledge,
                record -> nacknowledgerFactory.create(record, errorEmitter),
                Function.identity()
            );
        }

        private AloQueueListener loadQueueListener() {
            Map<String, Object> listenerConfig = config.modifyAndGetProperties(properties -> {});
            return AloQueueListenerConfig.load(listenerConfig, AloKafkaQueueListener.class)
                .orElseGet(AloQueueListener::noOp);
        }

        private AloFactory<ConsumerRecord<K, V>> loadAloFactory() {
            Map<String, Object> factoryConfig = config.modifyAndGetProperties(properties -> {});
            return AloFactoryConfig.loadDecorated(factoryConfig, AloKafkaConsumerRecordDecorator.class);
        }

        private long loadMaxInFlightPerSubscription() {
            return config.loadLong(MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG).orElse(DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION);
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

        private static AcknowledgementQueueMode loadAcknowledgementQueueMode(KafkaConfig config) {
            return config.loadEnum(ACKNOWLEDGEMENT_QUEUE_MODE_CONFIG, AcknowledgementQueueMode.class)
                .orElse(DEFAULT_ACKNOWLEDGEMENT_QUEUE_MODE);
        }

        private static String incrementId(String id) {
            return id + "-" + COUNTS_BY_ID.computeIfAbsent(id, __ -> new AtomicLong()).incrementAndGet();
        }
    }
}
