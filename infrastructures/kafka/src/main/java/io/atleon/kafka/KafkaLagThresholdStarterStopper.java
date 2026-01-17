package io.atleon.kafka;

import io.atleon.core.StarterStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link StarterStopper} that is based on periodically measuring the estimated lag of some
 * consumer group(s), usually downstream of the stream process that this would be applied to. This
 * is implemented using a periodic measurement of the total lag for any given consumer group(s).
 * When the lag measurement is measured at-or-below the "low tide" threshold, a "start" signal is
 * issued. When measurements of lag are above the configured "high tide" threshold, a "stop" signal
 * is issued.
 */
public class KafkaLagThresholdStarterStopper implements StarterStopper {

    private static final Duration DEFAULT_SAMPLE_INTERVAL = Duration.ofSeconds(5);

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLagThresholdStarterStopper.class);

    private final KafkaConfigSource configSource;

    private final Collection<String> consumerGroupIds;

    private final Duration sampleDelay;

    private final long highTide;

    private final long lowTide;

    private KafkaLagThresholdStarterStopper(
            KafkaConfigSource configSource,
            Collection<String> consumerGroupIds,
            Duration sampleDelay,
            long highTide,
            long lowTide) {
        this.configSource = configSource;
        this.consumerGroupIds = consumerGroupIds;
        this.sampleDelay = sampleDelay;
        this.highTide = highTide;
        this.lowTide = lowTide;
    }

    /**
     * Create a new {@link KafkaLagThresholdStarterStopper} based on the provided Config Source,
     * and monitoring the provided consumer group ID.
     *
     * @param configSource The source of configuration used to connect to Kafka
     * @param consumerGroupId The ID of the consumer group whose total lag is monitored
     * @return A new {@link KafkaLagThresholdStarterStopper}
     */
    public static KafkaLagThresholdStarterStopper create(KafkaConfigSource configSource, String consumerGroupId) {
        return create(configSource, Collections.singleton(consumerGroupId));
    }

    /**
     * Create a new {@link KafkaLagThresholdStarterStopper} based on the provided Config Source,
     * and monitoring the provided consumer group IDs.
     *
     * @param configSource The source of configuration used to connect to Kafka
     * @param consumerGroupIds The IDs of the consumer groups whose combined total lag are monitored
     * @return A new {@link KafkaLagThresholdStarterStopper}
     */
    public static KafkaLagThresholdStarterStopper create(
            KafkaConfigSource configSource, Collection<String> consumerGroupIds) {
        return new KafkaLagThresholdStarterStopper(configSource, consumerGroupIds, DEFAULT_SAMPLE_INTERVAL, 0, 0);
    }

    @Override
    public Flux<Boolean> startStop() {
        return configSource.create().flatMapMany(this::startStop);
    }

    /**
     * Configure the delay between total lag measurements.
     */
    public KafkaLagThresholdStarterStopper withSampleDelay(Duration sampleDelay) {
        return new KafkaLagThresholdStarterStopper(configSource, consumerGroupIds, sampleDelay, highTide, lowTide);
    }

    /**
     * Configure the threshold of lag above which a "stop" signal is issued, and at-or-below which
     * a "start" signal is issued.
     */
    public KafkaLagThresholdStarterStopper withThreshold(long lagThreshold) {
        return withThresholds(lagThreshold, lagThreshold);
    }

    /**
     * Configure the thresholds which are used to signal "start and "stop" signals. When the lag is
     * measured above the "high tide", a "stop" signal is issued. When the lag is measured
     * at-or-below the "low tide", a "start" signal is issued.
     */
    public KafkaLagThresholdStarterStopper withThresholds(long highTide, long lowTide) {
        if (highTide < lowTide) {
            throw new IllegalArgumentException("highTide must be greater-than-or-equal-to lowTide");
        }
        return new KafkaLagThresholdStarterStopper(configSource, consumerGroupIds, sampleDelay, highTide, lowTide);
    }

    private Flux<Boolean> startStop(KafkaConfig config) {
        return Flux.using(() -> ReactiveAdmin.create(config.nativeProperties()), this::startStop, ReactiveAdmin::close);
    }

    private Flux<Boolean> startStop(ReactiveAdmin admin) {
        return admin.listTopicPartitionGroupOffsets(consumerGroupIds)
                .reduce(0L, (sum, offsets) -> sum + offsets.estimateLag())
                .doOnNext(this::logCalculatedLag)
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, sampleDelay).doBeforeRetry(this::logCalculationFailure))
                .repeatWhen(it -> it.delayElements(sampleDelay))
                .scan(false, (started, totalLag) -> started ? totalLag <= highTide : totalLag <= lowTide)
                .skip(1)
                .distinctUntilChanged()
                .doOnNext(this::logStartStopSignal);
    }

    private void logCalculatedLag(long lag) {
        LOGGER.debug(
                "Calculated lag for consumerGroupsIds={} is {} where highTide={} and lowTide={}",
                consumerGroupIds,
                lag,
                highTide,
                lowTide);
    }

    private void logCalculationFailure(Retry.RetrySignal signal) {
        LOGGER.error(
                "Failed to calculate total lag for consumerGroupsIds={} where signal={}. This may cause stream hanging.",
                consumerGroupIds,
                signal);
    }

    private void logStartStopSignal(boolean start) {
        if (start) {
            LOGGER.info("Start: Lag for consumerGroupsIds={} is at-or-below lowTide={}", consumerGroupIds, lowTide);
        } else {
            LOGGER.warn("Stop: Lag for consumerGroupsIds={} is above highTide={}", consumerGroupIds, highTide);
        }
    }
}
