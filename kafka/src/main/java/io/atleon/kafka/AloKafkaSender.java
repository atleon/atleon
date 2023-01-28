package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import io.atleon.util.ConfigLoading;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A reactive Kafka sender with at-least-once semantics for producing records to topics of a Kafka
 * cluster.
 * <P>
 * At most one instance of a {@link KafkaSender} is kept and can be closed upon invoking
 * {@link AloKafkaSender#close()}. However, if after closing, more sent Publishers are subscribed
 * to, a new Sender instance will be created and cached.
 *
 * @param <K> outbound record key type
 * @param <V> outbound record value type
 */
public class AloKafkaSender<K, V> implements Closeable {

    /**
     * Prefix used on all AloKafkaSender-specific configurations
     */
    public static final String CONFIG_PREFIX = "kafka.sender.";

    /**
     * It may be desirable to have client IDs be incremented per Sender. This can remedy
     * conflicts with external resource registration (i.e. JMX) if the same client ID is expected
     * to be used concurrently by more than one Sender
     */
    public static final String AUTO_INCREMENT_CLIENT_ID_CONFIG = CONFIG_PREFIX + "auto.increment.client.id";

    /**
     * This is the maximum number of "in-flight" Records per sent Publisher. Note that this is
     * different from ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, which controls the
     * total number of in-flight Requests for all sent Publishers on a Sender/Producer. It is
     * important to note that if this is greater than 1 and a fatal error/cancellation occurs, the
     * SenderResults of other in-flight Records will NOT be delivered downstream, removing the
     * ability to execute auxiliary tasks on those Results, i.e. recovery from error or execution
     * of acknowledgement
     */
    public static final String MAX_IN_FLIGHT_PER_SEND_CONFIG = CONFIG_PREFIX + "max.in.flight.per.send";

    /**
     * Upon occurrence of synchronous Exceptions (i.e. serialization), sent Publishers are
     * immediately fatally errored/canceled. Upon asynchronous Exceptions (i.e. network issue),
     * We allow configuring whether to "stop" (aka error-out) the sent Publisher. Therefore, if
     * this is disabled, there can be slightly different behavior for synchronous vs. asynchronous
     * Exceptions. Most commonly, with non-singleton Publishers, the difference in behavior is
     * directly related to the max number of in-flight Records, i.e. if the max in-flight Records
     * is 1, there is no difference between this being enabled or disabled. If the max in-flight
     * messages is greater than 1, other in-flight SenderResults may not be delivered downstream
     * if this is enabled.
     */
    public static final String STOP_ON_ERROR_CONFIG = CONFIG_PREFIX + "stop.on.error";

    private static final Logger LOGGER = LoggerFactory.getLogger(AloKafkaSender.class);

    private static final boolean DEFAULT_AUTO_INCREMENT_CLIENT_ID = false;

    private static final int DEFAULT_MAX_IN_FLIGHT_PER_SEND = 256;

    private static final boolean DEFAULT_STOP_ON_ERROR = false;

    private static final Map<String, AtomicLong> COUNTS_BY_CLIENT_ID = new ConcurrentHashMap<>();

    private static final Map<String, Scheduler> SCHEDULERS_BY_CLIENT_ID = new ConcurrentHashMap<>();

    private final Mono<KafkaSender<K, V>> futureKafkaSender;

    private final Sinks.Many<Long> closeSink = Sinks.many().multicast().directBestEffort();

    private AloKafkaSender(KafkaConfigSource configSource) {
        this.futureKafkaSender = configSource.create()
            .<SenderOptions<K, V>>map(AloKafkaSender::newSenderOptions)
            .map(KafkaSender::create)
            .cacheInvalidateWhen(client -> closeSink.asFlux().next().then(), KafkaSender::close);
    }

    public static <K, V> AloKafkaSender<K, V> from(KafkaConfigSource configSource) {
        return new AloKafkaSender<>(configSource);
    }

    public Function<Publisher<V>, Flux<KafkaSenderResult<V>>>
    sendValues(String topic, Function<? super V, ? extends K> valueToKey) {
        return values -> sendValues(values, topic, valueToKey);
    }

    public Flux<KafkaSenderResult<V>>
    sendValues(Publisher<V> values, String topic, Function<? super V, ? extends K> valueToKey) {
        return futureKafkaSender
            .flatMapMany(sender -> sendValues(sender, values, value -> topic, valueToKey));
    }

    public Function<Publisher<V>, Flux<KafkaSenderResult<V>>> sendValues(
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey) {
        return values -> sendValues(values, valueToTopic, valueToKey);
    }

    public Flux<KafkaSenderResult<V>> sendValues(
        Publisher<V> values,
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey) {
        return futureKafkaSender
            .flatMapMany(sender -> sendValues(sender, values, valueToTopic, valueToKey));
    }

    public Mono<KafkaSenderResult<ProducerRecord<K, V>>> sendRecord(ProducerRecord<K, V> record) {
        return sendRecords(Flux.just(record)).next();
    }

    public Flux<KafkaSenderResult<ProducerRecord<K, V>>> sendRecords(Publisher<ProducerRecord<K, V>> records) {
        return futureKafkaSender.flatMapMany(sender -> sendRecords(sender, records));
    }

    public Function<Publisher<Alo<V>>, AloFlux<KafkaSenderResult<V>>>
    sendAloValues(String topic, Function<? super V, ? extends K> valueToKey) {
        return aloValues -> sendAloValues(aloValues, topic, valueToKey);
    }

    public AloFlux<KafkaSenderResult<V>>
    sendAloValues(Publisher<Alo<V>> aloValues, String topic, Function<? super V, ? extends K> valueToKey) {
        return sendAloValues(aloValues, value -> topic, valueToKey);
    }

    public Function<Publisher<Alo<V>>, AloFlux<KafkaSenderResult<V>>> sendAloValues(
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey) {
        return aloValues -> sendAloValues(aloValues, valueToTopic, valueToKey);
    }

    public AloFlux<KafkaSenderResult<V>> sendAloValues(
        Publisher<Alo<V>> aloValues,
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey) {
        return futureKafkaSender
            .flatMapMany(sender -> sendAloValues(sender, aloValues, valueToTopic, valueToKey))
            .as(AloFlux::wrap);
    }

    public AloFlux<KafkaSenderResult<ProducerRecord<K, V>>>
    sendAloRecords(Publisher<Alo<ProducerRecord<K, V>>> aloRecords) {
        return futureKafkaSender
            .flatMapMany(sender -> sendAloRecords(sender, aloRecords))
            .as(AloFlux::wrap);
    }

    public void close(Object reason) {
        LOGGER.info("Closing AloKafkaSender due to reason={}", reason);
        close();
    }

    @Override
    public void close() {
        closeSink.tryEmitNext(System.currentTimeMillis());
    }

    private Flux<KafkaSenderResult<V>> sendValues(
        KafkaSender<K, V> sender,
        Publisher<V> values,
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey) {
        return Flux.from(values)
            .map(aloValue -> createSenderRecordOfValue(aloValue, valueToTopic, valueToKey))
            .transform(sender::send)
            .map(KafkaSenderResult::fromSenderResult);
    }

    private SenderRecord<K, V, V> createSenderRecordOfValue(
        V value,
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey) {
        String topic = valueToTopic.apply(value);
        K key = valueToKey.apply(value);
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, null, key, value);
        return SenderRecord.create(producerRecord, value);
    }

    private Flux<KafkaSenderResult<ProducerRecord<K, V>>>
    sendRecords(KafkaSender<K, V> sender, Publisher<ProducerRecord<K, V>> records) {
        return Flux.from(records)
            .map(record -> SenderRecord.create(record, record))
            .transform(sender::send)
            .map(KafkaSenderResult::fromSenderResult);
    }

    private Flux<Alo<KafkaSenderResult<V>>> sendAloValues(
        KafkaSender<K, V> sender,
        Publisher<Alo<V>> aloValues,
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey) {
        return AloFlux.toFlux(aloValues)
            .map(aloValue -> createSenderRecordOfAloValue(aloValue, valueToTopic, valueToKey))
            .transform(sender::send)
            .map(KafkaSenderResult::fromSenderResultOfAlo);
    }

    private SenderRecord<K, V, Alo<V>> createSenderRecordOfAloValue(
        Alo<V> aloValue,
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey) {
        V value = aloValue.get();
        String topic = valueToTopic.apply(value);
        K key = valueToKey.apply(value);
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, null, key, value);
        return SenderRecord.create(producerRecord, aloValue);
    }

    private Flux<Alo<KafkaSenderResult<ProducerRecord<K, V>>>>
    sendAloRecords(KafkaSender<K, V> sender, Publisher<Alo<ProducerRecord<K, V>>> aloRecords) {
        return AloFlux.toFlux(aloRecords)
            .map(aloRecord -> SenderRecord.create(aloRecord.get(), aloRecord))
            .transform(sender::send)
            .map(KafkaSenderResult::fromSenderResultOfAlo);
    }

    private static <K, V> SenderOptions<K, V> newSenderOptions(Map<String, Object> config) {
        Map<String, Object> options = new HashMap<>();
        loadInto(options, config, AUTO_INCREMENT_CLIENT_ID_CONFIG, DEFAULT_AUTO_INCREMENT_CLIENT_ID);
        loadInto(options, config, MAX_IN_FLIGHT_PER_SEND_CONFIG, DEFAULT_MAX_IN_FLIGHT_PER_SEND);
        loadInto(options, config, STOP_ON_ERROR_CONFIG, DEFAULT_STOP_ON_ERROR);

        // 1) Create Producer config from remaining configs that are NOT options (help avoid WARNs)
        // 2) Note Client ID and, if enabled, increment client ID in consumer config
        Map<String, Object> producerConfig = new HashMap<>(config);
        options.keySet().forEach(producerConfig::remove);
        String clientId = ConfigLoading.loadOrThrow(config, CommonClientConfigs.CLIENT_ID_CONFIG, Object::toString);
        if (ConfigLoading.loadOrThrow(options, AUTO_INCREMENT_CLIENT_ID_CONFIG, Boolean::valueOf)) {
            producerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId + "-" + nextClientIdCount(clientId));
        }

        return SenderOptions.<K, V>create(producerConfig)
            .maxInFlight(ConfigLoading.loadOrThrow(options, MAX_IN_FLIGHT_PER_SEND_CONFIG, Integer::valueOf))
            .stopOnError(ConfigLoading.loadOrThrow(options, STOP_ON_ERROR_CONFIG, Boolean::valueOf))
            .scheduler(schedulerForClient(clientId));
    }

    private static long nextClientIdCount(String clientId) {
        return COUNTS_BY_CLIENT_ID.computeIfAbsent(clientId, key -> new AtomicLong()).incrementAndGet();
    }

    private static Scheduler schedulerForClient(String clientId) {
        return SCHEDULERS_BY_CLIENT_ID.computeIfAbsent(clientId, AloKafkaSender::newSchedulerForClient);
    }

    private static Scheduler newSchedulerForClient(String clientId) {
        return Schedulers.newSingle(AloKafkaSender.class.getSimpleName() + "-" + clientId);
    }

    private static void loadInto(Map<String, Object> destination, Map<String, Object> source, String key, Object def) {
        destination.put(key, source.getOrDefault(key, def));
    }
}
