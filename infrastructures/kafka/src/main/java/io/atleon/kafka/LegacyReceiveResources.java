package io.atleon.kafka;

import io.atleon.core.AcknowledgementQueueMode;
import io.atleon.core.Alo;
import io.atleon.core.AloComponentExtractor;
import io.atleon.core.AloFactory;
import io.atleon.core.AloFactoryConfig;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

final class LegacyReceiveResources<K, V> {

    private static final Map<String, AtomicLong> COUNTS_BY_ID = new ConcurrentHashMap<>();

    private final KafkaConfig config;

    private final NacknowledgerFactory<K, V> nacknowledgerFactory;

    private final AcknowledgementQueueMode acknowledgementQueueMode;

    public LegacyReceiveResources(KafkaConfig config) {
        this.config = config;
        this.nacknowledgerFactory = createNacknowledgerFactory(config);
        this.acknowledgementQueueMode = loadAcknowledgementQueueMode(config);
    }

    public Flux<Alo<ConsumerRecord<K, V>>>
    receive(OptionsInitializer<K, V> optionsInitializer, ConsumerMutexEnforcer consumerMutexEnforcer) {
        ErrorEmitter<Alo<ConsumerRecord<K, V>>> errorEmitter = newErrorEmitter();
        ReceiverOptions<K, V> options = newReceiverOptions(optionsInitializer);
        ConsumerMutexEnforcer.ProhibitableConsumerFactory consumerFactory = consumerMutexEnforcer.newProhibitableConsumerFactory();
        return KafkaReceiver.create(consumerFactory, options).receive()
            .transform(newAloQueueingTransformer(errorEmitter::safelyEmit))
            .transform(errorEmitter::applyTo)
            .transform(this::applySignalListenerFactories)
            .doFinally(__ -> consumerFactory.prohibitFurtherConsumption(options.closeTimeout().multipliedBy(2)));
    }

    private ErrorEmitter<Alo<ConsumerRecord<K, V>>> newErrorEmitter() {
        Duration timeout = config.loadDuration(AloKafkaReceiver.ERROR_EMISSION_TIMEOUT_CONFIG)
            .orElse(ErrorEmitter.DEFAULT_TIMEOUT);
        return ErrorEmitter.create(timeout);
    }

    private ReceiverOptions<K, V> newReceiverOptions(OptionsInitializer<K, V> optionsInitializer) {
        ReceiverOptions<K, V> defaultOptions = optionsInitializer.initialize(newConsumerConfig());
        return defaultOptions
            .pollTimeout(config.loadDuration(AloKafkaReceiver.POLL_TIMEOUT_CONFIG)
                .orElse(defaultOptions.pollTimeout()))
            .commitInterval(config.loadDuration(AloKafkaReceiver.COMMIT_INTERVAL_CONFIG)
                .orElse(defaultOptions.commitInterval()))
            .maxCommitAttempts(config.loadInt(AloKafkaReceiver.MAX_COMMIT_ATTEMPTS_CONFIG)
                .orElse(defaultOptions.maxCommitAttempts()))
            .closeTimeout(config.loadDuration(AloKafkaReceiver.CLOSE_TIMEOUT_CONFIG)
                .orElse(Duration.ofSeconds(30)))
            .maxDelayRebalance(loadMaxDelayRebalance().orElse(defaultOptions.maxDelayRebalance()));
    }

    private Map<String, Object> newConsumerConfig() {
        return config.modifyAndGetProperties(properties -> {
            // Remove any Atleon-specific config (helps avoid warning logs about unused config)
            properties.keySet().removeIf(key -> key.startsWith(AloKafkaReceiver.CONFIG_PREFIX));

            // If enabled, increment Client ID
            if (config.loadBoolean(AloKafkaReceiver.AUTO_INCREMENT_CLIENT_ID_CONFIG).orElse(false)) {
                properties.computeIfPresent(CommonClientConfigs.CLIENT_ID_CONFIG, (__, id) -> incrementId(id.toString()));
            }
        });
    }

    private Optional<Duration> loadMaxDelayRebalance() {
        Optional<Duration> maxDelayRebalance = config.loadDuration(AloKafkaReceiver.MAX_DELAY_REBALANCE_CONFIG);
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
        return config.loadLong(AloKafkaReceiver.MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG).orElse(4096L);
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
            loadNacknowledgerFactory(config, AloKafkaReceiver.NACKNOWLEDGER_TYPE_CONFIG, NacknowledgerFactory.class);
        return nacknowledgerFactory.orElseGet(NacknowledgerFactory.Emit::new);
    }

    private static <K, V, N extends NacknowledgerFactory<K, V>> Optional<NacknowledgerFactory<K, V>>
    loadNacknowledgerFactory(KafkaConfig config, String key, Class<N> type) {
        return config.loadConfiguredWithPredefinedTypes(key, type, LegacyReceiveResources::newPredefinedNacknowledgerFactory);
    }

    private static <K, V> Optional<NacknowledgerFactory<K, V>> newPredefinedNacknowledgerFactory(String typeName) {
        if (typeName.equalsIgnoreCase(AloKafkaReceiver.NACKNOWLEDGER_TYPE_EMIT)) {
            return Optional.of(new NacknowledgerFactory.Emit<>());
        } else {
            return Optional.empty();
        }
    }

    private static AcknowledgementQueueMode loadAcknowledgementQueueMode(KafkaConfig config) {
        return config.loadEnum(AloKafkaReceiver.ACKNOWLEDGEMENT_QUEUE_MODE_CONFIG, AcknowledgementQueueMode.class)
            .orElse(AcknowledgementQueueMode.STRICT);
    }

    private static String incrementId(String id) {
        return id + "-" + COUNTS_BY_ID.computeIfAbsent(id, __ -> new AtomicLong()).incrementAndGet();
    }

    public interface OptionsInitializer<K, V> {

        ReceiverOptions<K, V> initialize(Map<String, Object> consumerConfig);
    }
}
