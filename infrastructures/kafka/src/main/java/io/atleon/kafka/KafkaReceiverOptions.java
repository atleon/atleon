package io.atleon.kafka;

import io.atleon.core.AcknowledgementQueueMode;
import io.atleon.util.ConfigLoading;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Options used to configure the reactive reception of records from Kafka.
 *
 * @param <K> The type of keys in received records
 * @param <V> The type of values in received records
 */
public final class KafkaReceiverOptions<K, V> {

    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100L);

    private static final int DEFAULT_MAX_FULL_POLL_RECORDS_PREFETCH = 2;

    private static final long DEFAULT_MAX_ACTIVE_IN_FLIGHT = 4096;

    private static final Duration DEFAULT_COMMIT_PERIOD = Duration.ofSeconds(5L); // Kafka default

    private static final Duration DEFAULT_COMMIT_TIMEOUT = Duration.ofSeconds(60L); // Kafka default

    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 100;

    private static final Duration DEFAULT_REVOCATION_GRACE_PERIOD = Duration.ofSeconds(60L); // Legacy Reactor default

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30L); // Kafka default

    private final Function<Map<String, Object>, Consumer<K, V>> consumerFactory;

    private final ConsumerListenerFactory consumerListenerFactory;

    private final ReceptionListenerFactory receptionListenerFactory;

    private final PollStrategyFactory pollStrategyFactory;

    private final Supplier<Scheduler> auxiliarySchedulerSupplier;

    private final Map<String, Object> consumerProperties;

    private final int fullPollRecordsPrefetch;

    private final long maxActiveInFlight;

    private final Duration pollTimeout;

    private final AcknowledgementQueueMode acknowledgementQueueMode;

    private final int commitBatchSize;

    private final Duration commitPeriod;

    private final Duration commitTimeout;

    private final int maxCommitAttempts;

    private final boolean commitlessOffsets;

    private final Duration revocationGracePeriod;

    private final Duration closeTimeout;

    private KafkaReceiverOptions(
        Function<Map<String, Object>, Consumer<K, V>> consumerFactory,
        ConsumerListenerFactory consumerListenerFactory,
        ReceptionListenerFactory receptionListenerFactory,
        PollStrategyFactory pollStrategyFactory,
        Supplier<Scheduler> auxiliarySchedulerSupplier,
        Map<String, Object> consumerProperties,
        int fullPollRecordsPrefetch,
        long maxActiveInFlight,
        Duration pollTimeout,
        AcknowledgementQueueMode acknowledgementQueueMode,
        int commitBatchSize,
        Duration commitPeriod,
        Duration commitTimeout,
        int maxCommitAttempts,
        boolean commitlessOffsets,
        Duration revocationGracePeriod,
        Duration closeTimeout
    ) {
        this.consumerFactory = consumerFactory;
        this.consumerListenerFactory = consumerListenerFactory;
        this.receptionListenerFactory = receptionListenerFactory;
        this.pollStrategyFactory = pollStrategyFactory;
        this.auxiliarySchedulerSupplier = auxiliarySchedulerSupplier;
        this.consumerProperties = consumerProperties;
        this.fullPollRecordsPrefetch = fullPollRecordsPrefetch;
        this.maxActiveInFlight = maxActiveInFlight;
        this.pollTimeout = pollTimeout;
        this.acknowledgementQueueMode = acknowledgementQueueMode;
        this.commitBatchSize = commitBatchSize;
        this.commitPeriod = commitPeriod;
        this.commitTimeout = commitTimeout;
        this.maxCommitAttempts = maxCommitAttempts;
        this.commitlessOffsets = commitlessOffsets;
        this.revocationGracePeriod = revocationGracePeriod;
        this.closeTimeout = closeTimeout;
        validate();
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

    public KafkaReceiverOptions.Builder<K, V> toBuilder() {
        return new Builder<>(consumerFactory)
            .consumerListenerFactory(consumerListenerFactory)
            .receptionListenerFactory(receptionListenerFactory)
            .pollStrategyFactory(pollStrategyFactory)
            .auxiliarySchedulerSupplier(auxiliarySchedulerSupplier)
            .consumerProperties(consumerProperties)
            .fullPollRecordsPrefetch(fullPollRecordsPrefetch)
            .maxActiveInFlight(maxActiveInFlight)
            .pollTimeout(pollTimeout)
            .acknowledgementQueueMode(acknowledgementQueueMode)
            .commitBatchSize(commitBatchSize)
            .commitPeriod(commitPeriod)
            .commitTimeout(commitTimeout)
            .maxCommitAttempts(maxCommitAttempts)
            .commitlessOffsets(commitlessOffsets)
            .revocationGracePeriod(revocationGracePeriod)
            .closeTimeout(closeTimeout);
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
     * Creates a new {@link ConsumerListener} based on a reactive handle to the active Consumer
     * instance.
     */
    public ConsumerListener createConsumerListener(ConsumerInvocable invocable) {
        return consumerListenerFactory.create(invocable);
    }

    /**
     * @see Builder#receptionListenerFactory(ReceptionListenerFactory)
     */
    public ReceptionListener createReceptionListener() {
        return receptionListenerFactory.create();
    }

    /**
     * @see Builder#pollStrategyFactory(PollStrategyFactory)
     */
    public PollStrategy createPollStrategy() {
        return pollStrategyFactory.create();
    }

    /**
     * @see Builder#auxiliarySchedulerSupplier(Supplier)
     */
    public Scheduler createAuxiliaryScheduler() {
        return auxiliarySchedulerSupplier.get();
    }

    public String loadConsumerTaskLoopName() {
        return "atleon-kafka-receive-consumer-" + loadClientId();
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
     * @see Builder#maxActiveInFlight(long)
     */
    public long maxActiveInFlight() {
        return maxActiveInFlight;
    }

    /**
     * @see Builder#pollTimeout(Duration)
     */
    public Duration pollTimeout() {
        return pollTimeout;
    }

    /**
     * @see Builder#acknowledgementQueueMode(AcknowledgementQueueMode)
     */
    public AcknowledgementQueueMode acknowledgementQueueMode() {
        return acknowledgementQueueMode;
    }

    /**
     * @see Builder#commitBatchSize(int)
     */
    public int commitBatchSize() {
        return commitBatchSize;
    }

    /**
     * @see Builder#commitPeriod(Duration)
     */
    public Duration commitPeriod() {
        return commitPeriod;
    }

    /**
     * @see Builder#commitTimeout(Duration)
     */
    public Duration commitTimeout() {
        return commitTimeout;
    }

    /**
     * @see Builder#maxCommitAttempts(int)
     */
    public int maxCommitAttempts() {
        return maxCommitAttempts;
    }

    /**
     * @see Builder#commitlessOffsets(boolean)
     */
    public boolean commitlessOffsets() {
        return commitlessOffsets;
    }

    /**
     * @see Builder#revocationGracePeriod(Duration)
     */
    public Duration revocationGracePeriod() {
        return revocationGracePeriod;
    }

    /**
     * @see Builder#closeTimeout(Duration)
     */
    public Duration closeTimeout() {
        return closeTimeout;
    }

    //FUTURE Consider making ReactiveAdmin abstract and allow clients to set factory method
    ReactiveAdmin createAdmin() {
        return ReactiveAdmin.create(consumerProperties);
    }

    private void validate() {
        validatePositive(fullPollRecordsPrefetch, "fullPollRecordsPrefetch");
        validatePositive(maxActiveInFlight, "maxActiveInFlight");
        validatePositive(commitBatchSize, "commitBatchSize");
        validatePositive(commitPeriod, "commitPeriod");
        validatePositive(maxCommitAttempts, "maxCommitAttempts");
        validateNonNegative(revocationGracePeriod, "revocationGracePeriod");
    }

    private static void validateNonNegative(Duration value, String name) {
        if (value.isNegative()) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    private static void validatePositive(Duration value, String name) {
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static void validatePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    public static final class Builder<K, V> {

        private final Function<Map<String, Object>, Consumer<K, V>> consumerFactory;

        private ConsumerListenerFactory consumerListenerFactory = ConsumerListenerFactory.noOp();

        private ReceptionListenerFactory receptionListenerFactory = ReceptionListenerFactory.noOp();

        private PollStrategyFactory pollStrategyFactory = PollStrategyFactory.natural();

        private Supplier<Scheduler> auxiliarySchedulerSupplier = Schedulers::parallel;

        private Map<String, Object> consumerProperties = Collections.emptyMap();

        private int fullPollRecordsPrefetch = DEFAULT_MAX_FULL_POLL_RECORDS_PREFETCH;

        private long maxActiveInFlight = DEFAULT_MAX_ACTIVE_IN_FLIGHT;

        private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

        private AcknowledgementQueueMode acknowledgementQueueMode = AcknowledgementQueueMode.STRICT;

        private int commitBatchSize = Integer.MAX_VALUE;

        private Duration commitPeriod = DEFAULT_COMMIT_PERIOD;

        private Duration commitTimeout = DEFAULT_COMMIT_TIMEOUT;

        private int maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;

        private boolean commitlessOffsets = false;

        private Duration revocationGracePeriod = DEFAULT_REVOCATION_GRACE_PERIOD;

        private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

        private Builder(Function<Map<String, Object>, Consumer<K, V>> consumerFactory) {
            this.consumerFactory = consumerFactory;
        }

        /**
         * Configures a singleton {@link ConsumerListenerFactory} that wraps the provided
         * listener.
         */
        public Builder<K, V> consumerListener(ConsumerListener listener) {
            return consumerListenerFactory(ConsumerListenerFactory.singleton(listener));
        }

        /**
         * Configures the factory that's used to create {@link ConsumerListener} instances on a
         * per-reception, per-subscription basis. Defaults to no-op.
         */
        public Builder<K, V> consumerListenerFactory(ConsumerListenerFactory consumerListenerFactory) {
            this.consumerListenerFactory = consumerListenerFactory;
            return this;
        }

        /**
         * Configures a singleton {@link ReceptionListenerFactory} that wraps the provided
         * listener.
         */
        public Builder<K, V> receptionListener(ReceptionListener listener) {
            return receptionListenerFactory(ReceptionListenerFactory.singleton(listener));
        }

        /**
         * Configures the factory that's used to create {@link ReceptionListener} instances on a
         * per-reception, per-subscription basis. Defaults to no-op.
         */
        public Builder<K, V> receptionListenerFactory(ReceptionListenerFactory receptionListenerFactory) {
            this.receptionListenerFactory = receptionListenerFactory;
            return this;
        }

        /**
         * Configures the factory that's used to create a {@link PollStrategy} on a per-reception,
         * per-subscription basis. It can be useful to configure fair or sophisticated dynamic
         * strategies in order to guard against natural poll biases or to implement poll
         * prioritization. Defaults to "natural", where all assigned partitions are polled on each
         * polling cycle.
         */
        public Builder<K, V> pollStrategyFactory(PollStrategyFactory pollStrategyFactory) {
            this.pollStrategyFactory = pollStrategyFactory;
            return this;
        }

        /**
         * Configures the supplier of an "auxiliary" {@link Scheduler}, which is created and used
         * for non-blocking periodic and timeout tasks (and therefore must be time-capable).
         */
        public Builder<K, V> auxiliarySchedulerSupplier(Supplier<Scheduler> auxiliarySchedulerSupplier) {
            this.auxiliarySchedulerSupplier = auxiliarySchedulerSupplier;
            return this;
        }

        /**
         * Sets a single native {@link ConsumerConfig Kafka Consumer property} used to configure
         * created Consumer instances.
         */
        public Builder<K, V> consumerProperty(String key, Object value) {
            Map<String, Object> consumerProperties = new HashMap<>(this.consumerProperties);
            consumerProperties.put(key, value);
            return consumerProperties(consumerProperties);
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
         * Sets the maximum number of records that are allowed to have been activated and emitted
         * for downstream consumption and not yet acknowledged (positively or negatively).
         */
        public Builder<K, V> maxActiveInFlight(long maxActiveInFlight) {
            this.maxActiveInFlight = maxActiveInFlight;
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
         * Sets the mode of acknowledgement queuing for offsets that are allowed to be committed.
         * In STRICT mode, every offset of every received record is made eligible for commit. In
         * COMPACT mode, commitment of any given record's offset may be skipped if a record that is
         * sequentially after it has already been acknowledged.
         */
        public Builder<K, V> acknowledgementQueueMode(AcknowledgementQueueMode acknowledgementQueueMode) {
            this.acknowledgementQueueMode = acknowledgementQueueMode;
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
         * Sets the period of processing acknowledgement after which records acknowledged during a
         * given period will be committed. Every commit period starts with the (positive)
         * acknowledgement of a record and will terminate after the provided period has elapsed, at
         * which point any partitions with records acknowledged during the period will have the
         * latest acknowledged offsets committed. The next period begins with the next
         * acknowledgement of a record, and the process repeats. Note that longer periods may
         * result in higher re-processing likelihood on rebalance(s), whereas as shorter periods
         * may cause observable Consumer overhead due to frequent commits. Defaults to 5 seconds.
         */
        public Builder<K, V> commitPeriod(Duration commitPeriod) {
            this.commitPeriod = commitPeriod;
            return this;
        }

        /**
         * Sets the timeout applied to <i>synchronous</i> commit attempts. Defaults to 60 seconds.
         */
        public Builder<K, V> commitTimeout(Duration commitTimeout) {
            this.commitTimeout = commitTimeout;
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
         * Configures whether offset commitment is disabled, which can be useful for stateless
         * consumption.
         */
        public Builder<K, V> commitlessOffsets(boolean commitlessOffsets) {
            this.commitlessOffsets = commitlessOffsets;
            return this;
        }

        /**
         * Configures the maximum amount of time that will be awaited for in-flight records to be
         * acknowledged from a partition whose assignment is being revoked. The latest acknowledged
         * offsets from such a partition are then used to issue one last commit before
         * processing/rebalancing is allowed to continue. Note that if it is intended for record
         * acknowledgements to be skipped, this should be set to {@link Duration#ZERO zero} such
         * that consumption continuation does not wait on acknowledgement(s) that will never
         * happen.
         */
        public Builder<K, V> revocationGracePeriod(Duration revocationGracePeriod) {
            this.revocationGracePeriod = revocationGracePeriod;
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
                consumerListenerFactory,
                receptionListenerFactory,
                pollStrategyFactory,
                auxiliarySchedulerSupplier,
                consumerProperties,
                fullPollRecordsPrefetch,
                maxActiveInFlight,
                pollTimeout,
                acknowledgementQueueMode,
                commitBatchSize,
                commitPeriod,
                commitTimeout,
                maxCommitAttempts,
                commitlessOffsets,
                revocationGracePeriod,
                closeTimeout);
        }
    }
}
