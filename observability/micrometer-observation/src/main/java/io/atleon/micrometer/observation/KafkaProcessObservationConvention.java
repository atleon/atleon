package io.atleon.micrometer.observation;

import io.atleon.micrometer.observation.KafkaObservations.HighCardinalityTags;
import io.atleon.micrometer.observation.KafkaObservations.LowCardinalityTags;
import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;

import java.util.ArrayList;
import java.util.List;

/**
 * Convenient (i.e. non-generic) interface for reactive Kafka process observation conventions
 */
public interface KafkaProcessObservationConvention extends ObservationConvention<KafkaConsumeContext> {

    @Override
    default boolean supportsContext(Observation.Context context) {
        return context instanceof KafkaConsumeContext;
    }

    @Override
    default String getName() {
        return "atleon.kafka.process";
    }

    class Default implements KafkaProcessObservationConvention {

        public static final KafkaProcessObservationConvention INSTANCE = new Default();

        @Override
        public KeyValues getLowCardinalityKeyValues(KafkaConsumeContext context) {
            List<KeyValue> keyValues = new ArrayList<>();
            keyValues.add(LowCardinalityTags.MESSAGING_SYSTEM.withValue("kafka"));
            keyValues.add(LowCardinalityTags.MESSAGING_OPERATION_NAME.withValue("process"));
            keyValues.add(LowCardinalityTags.MESSAGING_OPERATION_TYPE.withValue("process"));
            context.clientIdValue(LowCardinalityTags.MESSAGING_CLIENT_ID).ifPresent(keyValues::add);
            keyValues.add(context.topicValue(LowCardinalityTags.MESSAGING_DESTINATION_NAME));
            keyValues.add(context.partitionValue(LowCardinalityTags.MESSAGING_DESTINATION_PARTITION_ID));
            return KeyValues.of(keyValues);
        }

        @Override
        public KeyValues getHighCardinalityKeyValues(KafkaConsumeContext context) {
            return KeyValues.of(context.offsetValue(HighCardinalityTags.MESSAGING_KAFKA_OFFSET));
        }
    }
}
