package io.atleon.micrometer.observation;

/**
 * Usage-specific extension of {@link KafkaConsumeObservationConvention} for polled records
 */
public interface KafkaPolledObservationConvention extends KafkaConsumeObservationConvention {

    @Override
    default String getName() {
        return "atleon.kafka.polled";
    }

    class Default implements KafkaPolledObservationConvention {

        public static final KafkaPolledObservationConvention INSTANCE = new Default();
    }
}
