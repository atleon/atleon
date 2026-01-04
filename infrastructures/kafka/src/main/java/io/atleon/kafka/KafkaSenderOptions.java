package io.atleon.kafka;

import io.atleon.util.ConfigLoading;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.util.concurrent.Queues;

/**
 * Options used to configure the reactive sending of records to Kafka.
 *
 * @param <K> The type of keys in sent records
 * @param <V> The type of values in sent records
 */
public final class KafkaSenderOptions<K, V> {

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30L); // Kafka default

    private final Function<Map<String, Object>, Producer<K, V>> producerFactory;

    private final ProducerListenerFactory producerListenerFactory;

    private final Map<String, Object> producerProperties;

    private final int maxInFlight;

    private final boolean sendImmediate;

    private final Duration closeTimeout;

    private KafkaSenderOptions(
            Function<Map<String, Object>, Producer<K, V>> producerFactory,
            ProducerListenerFactory producerListenerFactory,
            Map<String, Object> producerProperties,
            int maxInFlight,
            boolean sendImmediate,
            Duration closeTimeout) {
        this.producerFactory = producerFactory;
        this.producerListenerFactory = producerListenerFactory;
        this.producerProperties = producerProperties;
        this.maxInFlight = maxInFlight;
        this.sendImmediate = sendImmediate;
        this.closeTimeout = closeTimeout;
        validate();
    }

    public static <K, V> KafkaSenderOptions<K, V> defaultOptions() {
        return KafkaSenderOptions.<K, V>newBuilder().build();
    }

    public static <K, V> Builder<K, V> newBuilder() {
        return new Builder<>(KafkaProducer::new);
    }

    /**
     * Creates a new builder that will use the provided factory to create Consumer instances on a
     * per-sender basis.
     */
    public static <K, V> Builder<K, V> newBuilder(Function<Map<String, Object>, Producer<K, V>> producerFactory) {
        return new Builder<>(producerFactory);
    }

    /**
     * Creates a new Producer instance that will be used to send records to Kafka.
     */
    public Producer<K, V> createProducer() {
        Map<String, Object> sanitized = new LinkedHashMap<>(producerProperties);
        // #452: The default/intended usage of reactive senders is in the context of order-assuming
        // stream processes, and it is therefore desirable to ensure ordering maintenance in
        // production by default. It used to be the case that this required setting
        // ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 1; However, the introduction of
        // idempotence and its defaulting to true in Kafka 3.0 made it such that ordering is
        // maintained by default, even if the max in flight requests is (up to) its default value
        // (historically, 5). However, this behavior is not maintained if idempotence is not
        // explicitly enabled and the max in flight requests is greater than its default value, so
        // we ensure that this condition is caught by ensuring idempotence is explicitly set by
        // default.
        sanitized.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return producerFactory.apply(sanitized);
    }

    /**
     * Creates a new {@link ProducerListener} based on a reactive handle to the active Producer
     * instance.
     */
    public ProducerListener createProducerListener(ProducerInvocable invocable) {
        return producerListenerFactory.create(invocable);
    }

    public String loadProducerTaskLoopName() {
        return "atleon-kafka-send-producer-" + loadClientId();
    }

    public String loadClientId() {
        return ConfigLoading.loadStringOrThrow(producerProperties, CommonClientConfigs.CLIENT_ID_CONFIG);
    }

    /**
     * @see Builder#maxInFlight(int)
     */
    public int maxInFlight() {
        return maxInFlight;
    }

    /**
     * @see Builder#sendImmediate(boolean)
     */
    public boolean sendImmediate() {
        return sendImmediate;
    }

    /**
     * @see Builder#closeTimeout(Duration)
     */
    public Duration closeTimeout() {
        return closeTimeout;
    }

    private void validate() {
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("maxInFlight must be positive");
        }
    }

    public static final class Builder<K, V> {

        private final Function<Map<String, Object>, Producer<K, V>> producerFactory;

        private ProducerListenerFactory producerListenerFactory = ProducerListenerFactory.noOp();

        private Map<String, Object> producerProperties = Collections.emptyMap();

        private int maxInFlight = Queues.SMALL_BUFFER_SIZE;

        private boolean sendImmediate = false;

        private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

        private Builder(Function<Map<String, Object>, Producer<K, V>> producerFactory) {
            this.producerFactory = producerFactory;
        }

        /**
         * Configures a singleton {@link ProducerListenerFactory} that wraps the provided
         * listener.
         */
        public Builder<K, V> producerListener(ProducerListener producerListener) {
            return producerListenerFactory(ProducerListenerFactory.singleton(producerListener));
        }

        /**
         * Configures the factory that's used to create {@link ProducerListener} instances on a
         * per-sender basis. Defaults to no-op.
         */
        public Builder<K, V> producerListenerFactory(ProducerListenerFactory producerListenerFactory) {
            this.producerListenerFactory = producerListenerFactory;
            return this;
        }

        /**
         * Sets a single native {@link ProducerConfig Kafka Producer property} used to configure
         * created Producer instances.
         */
        public Builder<K, V> producerProperty(String key, Object value) {
            Map<String, Object> consumerProperties = new HashMap<>(this.producerProperties);
            consumerProperties.put(key, value);
            return producerProperties(consumerProperties);
        }

        /**
         * Sets the native {@link ProducerConfig Kafka Producer properites} that are used to
         * configure created Producer instances.
         */
        public Builder<K, V> producerProperties(Map<String, Object> producerProperties) {
            this.producerProperties = producerProperties;
            return this;
        }

        /**
         * Sets the maximum number of records that are allowed to have their sending initiated, and
         * for which we are awaiting a result (of either success or failure).
         */
        public Builder<K, V> maxInFlight(int maxInFlight) {
            this.maxInFlight = maxInFlight;
            return this;
        }

        /**
         * By default, invocations of {@link Producer#send(ProducerRecord, Callback)} have their
         * execution serialized (using a single-threaded {@link java.util.concurrent.Executor})
         * in a Producer-specific task loop. In general, this isolation is desirable because it is
         * possible for send invocations to block. For example:
         * - Waiting on metadata (for up to {@link ProducerConfig#MAX_BLOCK_MS_CONFIG})
         * - I/O-bound serialization (e.g. Avro schema registry integration)
         * - Waiting to append to outbound record buffer
         * In some cases, it may be safe and optimal to skip the task loop and immediately invoke
         * send(s) on sending/publishing threads, for example if those threads are already amenable
         * to blocking (i.e. "elastic" threads). This optimization may increase throughput capacity
         * by leveraging the underlying Producer's ability to support multithreaded invocation.
         */
        public Builder<K, V> sendImmediate(boolean sendImmediate) {
            this.sendImmediate = sendImmediate;
            return this;
        }

        /**
         * Sets the timeout used on invocations to {@link Producer#close(Duration)}.
         */
        public Builder<K, V> closeTimeout(Duration closeTimeout) {
            this.closeTimeout = closeTimeout;
            return this;
        }

        public KafkaSenderOptions<K, V> build() {
            return new KafkaSenderOptions<>(
                    producerFactory,
                    producerListenerFactory,
                    producerProperties,
                    maxInFlight,
                    sendImmediate,
                    closeTimeout);
        }
    }
}
