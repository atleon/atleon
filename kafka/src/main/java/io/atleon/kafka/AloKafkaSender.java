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
 * <p>
 * Each sent record produces a {@link KafkaSenderResult}. These results may not be emitted in the
 * same order that their corresponding records are sent.
 * <p>
 * At most one instance of a {@link KafkaSender} is kept and can be closed upon invoking
 * {@link AloKafkaSender#close()}. However, if after closing, more sent Publishers are subscribed
 * to, a new Sender instance will be created and cached.
 *
 * @param <K> outbound record key type
 * @param <V> outbound record value type
 * @see KafkaSender#send(Publisher)
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

    private final Mono<Resources<K, V>> futureResources;

    private final Sinks.Many<Long> closeSink = Sinks.many().multicast().directBestEffort();

    private AloKafkaSender(KafkaConfigSource configSource) {
        this.futureResources = configSource.create()
            .map(Resources::<K, V>fromConfig)
            .cacheInvalidateWhen(client -> closeSink.asFlux().next().then(), Resources::close);
    }

    /**
     * Creates a new AloKafkaReceiver from the provided {@link KafkaConfigSource}
     *
     * @param configSource The reactive source of Kafka Sender properties
     * @param <K>          The types of keys contained in sent records
     * @param <V>          The types of values contained in sent records
     * @return A new AloKafkaSender
     */
    public static <K, V> AloKafkaSender<K, V> from(KafkaConfigSource configSource) {
        return new AloKafkaSender<>(configSource);
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of record values to a
     * Publisher of the results of sending each record value. See
     * {@link #sendValues(Publisher, String, Function)} for further information.
     *
     * @param topic      The topic to which produced records will be sent
     * @param valueToKey {@link Function} that determines record key based on record value
     * @return a {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<V>, Flux<KafkaSenderResult<V>>> sendValues(
        String topic,
        Function<? super V, ? extends K> valueToKey
    ) {
        return values -> sendValues(values, topic, valueToKey);
    }

    /**
     * Sends a sequence of values to be populated in Kafka {@link ProducerRecord}s to the provided
     * Kafka topic. The key of each record is extracted from each sent value using the provided
     * function.
     * <p>
     * The output of each sent value is a {@link KafkaSenderResult} containing the sent value.
     *
     * @param values     A Publisher of Kafka record values
     * @param topic      The topic to which produced records will be sent
     * @param valueToKey {@link Function} that determines record key based on record value
     * @return a Publisher of the results of each sent record value
     */
    public Flux<KafkaSenderResult<V>> sendValues(
        Publisher<V> values,
        String topic,
        Function<? super V, ? extends K> valueToKey
    ) {
        return sendValues(values, value -> topic, valueToKey);
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of record values to a
     * Publisher of the results of sending each record value. See
     * {@link #sendValues(Publisher, Function, Function)} for further information.
     *
     * @param valueToTopic {@link Function} that determines destination topic based on record value
     * @param valueToKey   {@link Function} that determines record key based on record value
     * @return a {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<V>, Flux<KafkaSenderResult<V>>> sendValues(
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey
    ) {
        return values -> sendValues(values, valueToTopic, valueToKey);
    }

    /**
     * Sends a sequence of values to be populated in Kafka {@link ProducerRecord}s. The destination
     * topic and key of each record is extracted from each sent value using the provided functions.
     * <p>
     * The output of each sent value is a {@link KafkaSenderResult} containing the sent value.
     *
     * @param values       A Publisher of Kafka record values
     * @param valueToTopic {@link Function} that determines destination topic based on record value
     * @param valueToKey   {@link Function} that determines record key based on record value
     * @return a Publisher of the results of each sent record value
     */
    public Flux<KafkaSenderResult<V>> sendValues(
        Publisher<V> values,
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey
    ) {
        Function<V, ProducerRecord<K, V>> recordCreator = newValueBasedRecordCreator(valueToTopic, valueToKey);
        return futureResources.flatMapMany(resources -> resources.send(values, recordCreator));
    }

    /**
     * Send a single {@link ProducerRecord}.
     *
     * @param record A record to send
     * @return A Publisher of the result of sending the record
     */
    public Mono<KafkaSenderResult<ProducerRecord<K, V>>> sendRecord(ProducerRecord<K, V> record) {
        return sendRecords(Flux.just(record)).next();
    }

    /**
     * Sends a sequence of {@link ProducerRecord}s
     * <p>
     * The output of each sent record is a {@link KafkaSenderResult} containing the sent record.
     *
     * @param records A Publisher of records to send
     * @return A Publisher of items referencing the result of each sent record
     */
    public Flux<KafkaSenderResult<ProducerRecord<K, V>>> sendRecords(Publisher<ProducerRecord<K, V>> records) {
        return futureResources.flatMapMany(resources -> resources.send(records, Function.identity()));
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of {@link Alo} items
     * referencing record values to a Publisher of Alo items referencing the result of sending each
     * record value. See {@link #sendAloValues(Publisher, String, Function)} for further
     * information.
     *
     * @param topic      The topic to which produced records will be sent
     * @param valueToKey {@link Function} that determines record key based on record value
     * @return A {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<Alo<V>>, AloFlux<KafkaSenderResult<V>>> sendAloValues(
        String topic,
        Function<? super V, ? extends K> valueToKey
    ) {
        return aloValues -> sendAloValues(aloValues, topic, valueToKey);
    }

    /**
     * Sends a sequence of {@link Alo} items referencing values to be populated in Kafka
     * {@link ProducerRecord}s to the provided Kafka topic. The key of each record is extracted
     * from each sent value using the provided function.
     * <p>
     * The output of each sent value is a {@link KafkaSenderResult} containing the sent value.
     * Each emitted item is an {@link Alo} item referencing a {@link KafkaSenderResult} and must be
     * acknowledged or nacknowledged such that its processing can be marked complete at the origin
     * of the record.
     *
     * @param aloValues  A Publisher of Alo items referencing Kafka record values
     * @param topic      The topic to which produced records will be sent
     * @param valueToKey {@link Function} that determines record key based on record value
     * @return a Publisher of Alo items referencing the result of each sent record value
     */
    public AloFlux<KafkaSenderResult<V>> sendAloValues(
        Publisher<Alo<V>> aloValues,
        String topic,
        Function<? super V, ? extends K> valueToKey
    ) {
        return sendAloValues(aloValues, value -> topic, valueToKey);
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of {@link Alo} items
     * referencing record values to a Publisher of Alo items referencing the result of sending each
     * record value. See {@link #sendAloValues(Publisher, Function, Function)} for further
     * information.
     *
     * @param valueToTopic {@link Function} that determines destination topic based on record value
     * @param valueToKey   {@link Function} that determines record key based on record value
     * @return a {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<Alo<V>>, AloFlux<KafkaSenderResult<V>>> sendAloValues(
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey
    ) {
        return aloValues -> sendAloValues(aloValues, valueToTopic, valueToKey);
    }

    /**
     * Sends a sequence of {@link Alo} items referencing values to be populated in Kafka
     * {@link ProducerRecord}s. The destination topic and key of each record is extracted from each
     * sent value using the provided functions.
     * <p>
     * The output of each sent value is a {@link KafkaSenderResult} containing the sent value.
     * Each emitted item is an {@link Alo} item referencing a {@link KafkaSenderResult} and must be
     * acknowledged or nacknowledged such that its processing can be marked complete at the origin
     * of the record.
     *
     * @param aloValues    A Publisher of Alo items referencing Kafka record values
     * @param valueToTopic {@link Function} that determines destination topic based on record value
     * @param valueToKey   {@link Function} that determines record key based on record value
     * @return a Publisher of Alo items referencing the result of each sent record value
     */
    public AloFlux<KafkaSenderResult<V>> sendAloValues(
        Publisher<Alo<V>> aloValues,
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey
    ) {
        Function<V, ProducerRecord<K, V>> recordCreator = newValueBasedRecordCreator(valueToTopic, valueToKey);
        return futureResources.flatMapMany(resources -> resources.sendAlos(aloValues, recordCreator)).as(AloFlux::wrap);
    }

    /**
     * Sends a sequence of {@link Alo} items referencing Kafka {@link ProducerRecord}s.
     * <p>
     * The output of each sent record is a {@link KafkaSenderResult} containing the sent record.
     * Each emitted item is an {@link Alo} item referencing a {@link KafkaSenderResult} and must be
     * acknowledged or nacknowledged such that its processing can be marked complete at the origin
     * of the record.
     *
     * @param aloRecords A Publisher of Alo items referencing records to send
     * @return A Publisher of Alo items referencing the result of each sent record
     */
    public AloFlux<KafkaSenderResult<ProducerRecord<K, V>>> sendAloRecords(Publisher<Alo<ProducerRecord<K, V>>> aloRecords) {
        return futureResources
            .flatMapMany(resources -> resources.sendAlos(aloRecords, Function.identity()))
            .as(AloFlux::wrap);
    }

    /**
     * Closes this sender and logs the provided reason.
     *
     * @param reason The reason this sender is being closed
     */
    public void close(Object reason) {
        LOGGER.info("Closing AloKafkaSender due to reason={}", reason);
        close();
    }

    @Override
    public void close() {
        closeSink.tryEmitNext(System.currentTimeMillis());
    }

    private Function<V, ProducerRecord<K, V>> newValueBasedRecordCreator(
        Function<? super V, String> valueToTopic,
        Function<? super V, ? extends K> valueToKey
    ) {
        return value -> new ProducerRecord<>(valueToTopic.apply(value), null, valueToKey.apply(value), value);
    }

    private static final class Resources<K, V> {

        private final KafkaSender<K, V> sender;

        private Resources(KafkaSender<K, V> sender) {
            this.sender = sender;
        }

        public static <K, V> Resources<K, V> fromConfig(Map<String, Object> config) {
            return new Resources<>(KafkaSender.create(ContextualProducerFactory.INSTANCE, newSenderOptions(config)));
        }

        public <T> Flux<KafkaSenderResult<T>> send(
            Publisher<T> publisher,
            Function<T, ProducerRecord<K, V>> recordCreator
        ) {
            return Flux.from(publisher)
                .map(item -> SenderRecord.create(recordCreator.apply(item), item))
                .transform(sender::send)
                .map(KafkaSenderResult::fromSenderResult);
        }

        public <T> Flux<Alo<KafkaSenderResult<T>>> sendAlos(
            Publisher<Alo<T>> alos,
            Function<T, ProducerRecord<K, V>> recordCreator
        ) {
            return AloFlux.toFlux(alos)
                .map(alo -> SenderRecord.create(recordCreator.apply(alo.get()), alo))
                .transform(sender::send)
                .map(KafkaSenderResult::fromSenderResultOfAlo);
        }

        public void close() {
            sender.close();
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
                .stopOnError(ConfigLoading.loadOrThrow(options, STOP_ON_ERROR_CONFIG, Boolean::valueOf));
        }

        private static long nextClientIdCount(String clientId) {
            return COUNTS_BY_CLIENT_ID.computeIfAbsent(clientId, key -> new AtomicLong()).incrementAndGet();
        }

        private static void loadInto(Map<String, Object> destination, Map<String, Object> source, String key, Object def) {
            destination.put(key, source.getOrDefault(key, def));
        }
    }
}
