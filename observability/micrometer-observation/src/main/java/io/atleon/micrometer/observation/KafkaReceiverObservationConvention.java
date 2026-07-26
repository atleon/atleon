package io.atleon.micrometer.observation;

import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;

/**
 * Convenient (i.e. non-generic) base interface for reactive Kafka reception
 */
public interface KafkaReceiverObservationConvention extends ObservationConvention<KafkaReceiverContext> {

    @Override
    default boolean supportsContext(Observation.Context context) {
        return context instanceof KafkaReceiverContext;
    }

    @Override
    default String getName() {
        return "atleon.receive.kafka";
    }

    class Default implements KafkaReceiverObservationConvention {

        private static final KafkaReceiverObservationConvention INSTANCE = new Default();

        public static KafkaReceiverObservationConvention instance() {
            return INSTANCE;
        }

        @Override
        public KeyValues getLowCardinalityKeyValues(KafkaReceiverContext context) {
            KeyValues keyValues = KeyValues.of(
                    KafkaObservation.ReceiverLowCardinalityTags.TOPIC.withValue(context.getTopic()),
                    KafkaObservation.ReceiverLowCardinalityTags.PARTITION.withValue(context.getPartitionAsString()));

            String clientId = context.getClientId();
            if (clientId != null) {
                keyValues = keyValues.and(KafkaObservation.ReceiverLowCardinalityTags.CLIENT_ID.withValue(clientId));
            }

            return keyValues;
        }

        @Override
        public KeyValues getHighCardinalityKeyValues(KafkaReceiverContext context) {
            return KeyValues.of(
                    KafkaObservation.ReceiverHighCardinalityTags.OFFSET.withValue(context.getOffsetAsString()));
        }
    }
}
