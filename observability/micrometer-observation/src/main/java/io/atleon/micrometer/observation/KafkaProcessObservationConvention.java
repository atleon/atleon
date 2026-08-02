package io.atleon.micrometer.observation;

/**
 * Usage-specific extension of {@link KafkaConsumeObservationConvention} for record processing
 */
public interface KafkaProcessObservationConvention extends KafkaConsumeObservationConvention {

    @Override
    default String getName() {
        return "atleon.kafka.process";
    }

    class Default implements KafkaProcessObservationConvention {

        public static final KafkaProcessObservationConvention INSTANCE = new Default();
    }
}
