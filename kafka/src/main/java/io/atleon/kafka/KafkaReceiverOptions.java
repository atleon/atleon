package io.atleon.kafka;

import io.atleon.util.ConfigLoading;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Configuration options used to configure the reactive reception of records from Kafka.
 *
 * @param <K> The type of keys in received records
 * @param <V> The type of values in received records
 */
public final class KafkaReceiverOptions<K, V> {

    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

    private static final int DEFAULT_MAX_FULL_POLL_RECORDS_PREFETCH = 2;

    private static final Duration DEFAULT_PROCESSING_GRACE_PERIOD = Duration.ofSeconds(30);

    private static final Duration DEFAULT_COMMIT_INTERVAL = Duration.ofSeconds(5); // Kafka default

    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 100;

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30L); // Kafka default

    private final Function<Map<String, Object>, Consumer<K, V>> consumerFactory;

    private final ReceptionListenerFactory listenerFactory;

    private final Map<String, Object> consumerProperties;

    private final int fullPollRecordsPrefetch;

    private final Duration pollTimeout;

    private final int commitBatchSize;

    private final Duration commitInterval;

    private final int maxCommitAttempts;

    private final Duration deactivationGracePeriod;

    private final Duration closeTimeout;

    private KafkaReceiverOptions(
        Function<Map<String, Object>, Consumer<K, V>> consumerFactory,
        ReceptionListenerFactory listenerFactory,
        Map<String, Object> consumerProperties,
        int fullPollRecordsPrefetch,
        Duration pollTimeout,
        int commitBatchSize,
        Duration commitInterval,
        int maxCommitAttempts,
        Duration deactivationGracePeriod,
        Duration closeTimeout
    ) {
        this.consumerFactory = consumerFactory;
        this.listenerFactory = listenerFactory;
        this.consumerProperties = consumerProperties;
        this.fullPollRecordsPrefetch = fullPollRecordsPrefetch;
        this.pollTimeout = pollTimeout;
        this.commitBatchSize = commitBatchSize;
        this.commitInterval = commitInterval;
        this.maxCommitAttempts = maxCommitAttempts;
        this.deactivationGracePeriod = deactivationGracePeriod;
        this.closeTimeout = closeTimeout;
    }

    public static <K, V> KafkaReceiverOptions<K, V> defaultOptions() {
        return KafkaReceiverOptions.<K, V>newBuilder().build();
    }

    public static <K, V> KafkaReceiverOptions.Builder<K, V> newBuilder() {
        return KafkaReceiverOptions.newBuilder(KafkaConsumer::new);
    }

    /**
     * Creates a new builder that will use the provided factory to create Consumer instances on a
     * per-reception, per-subscription basis.
     */
    public static <K, V> KafkaReceiverOptions.Builder<K, V> newBuilder(
        Function<Map<String, Object>, Consumer<K, V>> consumerFactory
    ) {
        return new Builder<>(consumerFactory);
    }

    /**
     * Creates a new Consumer instance that will be used when subscribing to receiver records.
     */
    public Consumer<K, V> createConsumer() {
        Map<String, Object> sanitized = new LinkedHashMap<>(consumerProperties);
        // We take control of offset committing, so force (or at least attempt to force) the native
        // periodic commit to be disabled.
        sanitized.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return consumerFactory.apply(sanitized);
    }

    /**
     * Creates a new reception listener based on a reactive handle to the active Consumer instance.
     */
    public ReceptionListener createReceptionListener(ConsumerInvocable invocable) {
        return listenerFactory.create(invocable);
    }

    public String loadClientId() {
        return ConfigLoading.loadStringOrThrow(consumerProperties, CommonClientConfigs.CLIENT_ID_CONFIG);
    }

    /**
     * @see Builder#fullPollRecordsPrefetch(int)
     */
    public int calculateMaxRecordsPrefetch() {
        return loadMaxPollRecords() * fullPollRecordsPrefetch;
    }

    public int loadMaxPollRecords() {
        return ConfigLoading.loadInt(consumerProperties, ConsumerConfig.MAX_POLL_RECORDS_CONFIG)
            .orElse(ConsumerConfig.DEFAULT_MAX_POLL_RECORDS);
    }

    /**
     * @see Builder#fullPollRecordsPrefetch(int)
     */
    public int fullPollRecordsPrefetch() {
        return fullPollRecordsPrefetch;
    }

    /**
     * @see Builder#pollTimeout(Duration)
     */
    public Duration pollTimeout() {
        return pollTimeout;
    }

    /**
     * @see Builder#commitBatchSize(int)
     */
    public int commitBatchSize() {
        return commitBatchSize;
    }

    /**
     * @see Builder#commitInterval(Duration)
     */
    public Duration commitInterval() {
        return commitInterval;
    }

    /**
     * @see Builder#maxCommitAttempts(int)
     */
    public int maxCommitAttempts() {
        return maxCommitAttempts;
    }

    /**
     * @see Builder#deactivationGracePeriod(Duration)
     */
    public Duration deactivationGracePeriod() {
        return deactivationGracePeriod;
    }

    /**
     * @see Builder#closeTimeout(Duration)
     */
    public Duration closeTimeout() {
        return closeTimeout;
    }

    public static final class Builder<K, V> {

        private final Function<Map<String, Object>, Consumer<K, V>> consumerFactory;

        private ReceptionListenerFactory listenerFactory = ReceptionListenerFactory.noOp();

        private Map<String, Object> consumerProperties = Collections.emptyMap();

        private int fullPollRecordsPrefetch = DEFAULT_MAX_FULL_POLL_RECORDS_PREFETCH;

        private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

        private int commitBatchSize = Integer.MAX_VALUE;

        private Duration commitInterval = DEFAULT_COMMIT_INTERVAL;

        private int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;

        private Duration deactivationGracePeriod = DEFAULT_PROCESSING_GRACE_PERIOD;

        private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

        private Builder(Function<Map<String, Object>, Consumer<K, V>> consumerFactory) {
            this.consumerFactory = consumerFactory;
        }

        /**
         * Configures a singleton {@link ReceptionListenerFactory} that wraps the provided
         * listener.
         */
        public Builder<K, V> listener(ReceptionListener listener) {
            return listenerFactory(ReceptionListenerFactory.singleton(listener));
        }

        /**
         * Configures the factory that's used to create {@link ReceptionListener} instances on a
         * per-reception, per-subscription basis. Defaults to no-op.
         */
        public Builder<K, V> listenerFactory(ReceptionListenerFactory listenerFactory) {
            this.listenerFactory = listenerFactory;
            return this;
        }

        /**
         * Sets the native {@link ConsumerConfig Kafka Consumer properites} that are used to
         * configure created Consumer instances.
         */
        public Builder<K, V> consumerProperties(Map<String, Object> consumerProperties) {
            this.consumerProperties = consumerProperties;
            return this;
        }

        /**
         * Sets the maximum number of "full" polled record batches that may be prefetched (awaiting
         * emission). The notion of "full" batches is related to
         * {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} and is decoupled from the size of batches
         * actually polled, since it is common that such polled batches are not full. Therefore,
         * these two configurations are multiplied by each other to calculate the maximum total
         * number of records that may be prefetched. Defaults to 2.
         */
        public Builder<K, V> fullPollRecordsPrefetch(int fullPollRecordsPrefetch) {
            this.fullPollRecordsPrefetch = fullPollRecordsPrefetch;
            return this;
        }

        /**
         * Sets the timeout used on invocations to {@link Consumer#poll(Duration)}.
         */
        public Builder<K, V> pollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
            return this;
        }

        /**
         * Sets a number of record acknowledgements that will trigger offset commit scheduling.
         * Defaults to {@link Integer#MAX_VALUE} (so commits solely happen periodically).
         */
        public Builder<K, V> commitBatchSize(int commitBatchSize) {
            this.commitBatchSize = commitBatchSize;
            return this;
        }

        /**
         * Sets the frequency at which acknowledged offsets will be scheduled for commit. Longer
         * intervals may result in higher re-processing on rebalance, whereas as shorter intervals
         * may cause observable Consumer overhead due to frequent commits. Defaults to 5 seconds.
         */
        public Builder<K, V> commitInterval(Duration commitInterval) {
            this.commitInterval = commitInterval;
            return this;
        }

        /**
         * Sets the maximum number of attempts that will be made to acknowledge offsets for any
         * given assigned partition. This is a consecutive commit attempt measurement, so upon
         * successful offset committing, the attempt count for the associated partition(s) is
         * reset.
         */
        public Builder<K, V> maxCommitAttempts(int maxCommitAttempts) {
            this.maxCommitAttempts = maxCommitAttempts;
            return this;
        }

        /**
         * Configures the maximum amount of time that will be awaited for in-flight records to be
         * acknowledged from a partition whose consumption is being deactivated, i.e. due to
         * revocation. The latest acknowledged offsets from such a partition are then used to issue
         * one last commit before processing/rebalancing is allowed to continue. Note that if it is
         * intended for record acknowledgements to be skipped, this should be set to
         * {@link Duration#ZERO zero} such that consumption continuation does not wait on
         * acknowledgement(s) that will never happen.
         */
        public Builder<K, V> deactivationGracePeriod(Duration deactivationGracePeriod) {
            this.deactivationGracePeriod = deactivationGracePeriod;
            return this;
        }

        /**
         * Configures the timeout used on invocations to {@link Consumer#close(Duration)}, which is
         * invoked upon either downstream cancellation or reception errors.
         */
        public Builder<K, V> closeTimeout(Duration closeTimeout) {
            this.closeTimeout = closeTimeout;
            return this;
        }

        public KafkaReceiverOptions<K, V> build() {
            return new KafkaReceiverOptions<>(
                consumerFactory,
                listenerFactory,
                consumerProperties,
                fullPollRecordsPrefetch,
                pollTimeout,
                commitBatchSize,
                commitInterval,
                maxCommitAttempts,
                deactivationGracePeriod,
                closeTimeout);
        }
    }
}
