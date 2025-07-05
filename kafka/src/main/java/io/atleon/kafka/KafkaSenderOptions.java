package io.atleon.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

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

    private final Duration closeTimeout;

    private KafkaSenderOptions(
        Function<Map<String, Object>, Producer<K, V>> producerFactory,
        ProducerListenerFactory producerListenerFactory,
        Map<String, Object> producerProperties,
        int maxInFlight,
        Duration closeTimeout
    ) {
        this.producerFactory = producerFactory;
        this.producerListenerFactory = producerListenerFactory;
        this.producerProperties = producerProperties;
        this.maxInFlight = maxInFlight;
        this.closeTimeout = closeTimeout;
    }

    public static <K, V> KafkaSenderOptions<K, V> defaultOptions() {
        return KafkaSenderOptions.<K, V>newBuilder().build();
    }

    public static <K, V> Builder<K, V> newBuilder() {
        return new Builder<>(config -> new ContextualProducer<>(new KafkaProducer<>(config)));
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
        return producerFactory.apply(producerProperties);
    }

    /**
     * Creates a new {@link ProducerListener} based on a reactive handle to the active Producer
     * instance.
     */
    public ProducerListener createProducerListener(ProducerInvocable invocable) {
        return producerListenerFactory.create(invocable);
    }

    /**
     * @see Builder#maxInFlight(int)
     */
    public int maxInFlight() {
        return maxInFlight;
    }

    /**
     * @see Builder#closeTimeout(Duration)
     */
    public Duration closeTimeout() {
        return closeTimeout;
    }

    public static final class Builder<K, V> {

        private final Function<Map<String, Object>, Producer<K, V>> producerFactory;

        private ProducerListenerFactory producerListenerFactory = ProducerListenerFactory.noOp();

        private Map<String, Object> producerProperties = Collections.emptyMap();

        private int maxInFlight = Queues.SMALL_BUFFER_SIZE;

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
         * Sets the native {@link ProducerConfig Kafka Consumer properites} that are used to
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
                closeTimeout
            );
        }
    }
}
