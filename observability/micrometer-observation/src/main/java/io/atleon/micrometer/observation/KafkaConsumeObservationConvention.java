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
 * Convenient (i.e. non-generic) interface for Kafka consumption observation conventions
 */
public interface KafkaConsumeObservationConvention extends ObservationConvention<KafkaConsumeContext> {

    @Override
    default boolean supportsContext(Observation.Context context) {
        return context instanceof KafkaConsumeContext;
    }

    @Override
    default KeyValues getLowCardinalityKeyValues(KafkaConsumeContext context) {
        List<KeyValue> keyValues = new ArrayList<>();
        keyValues.add(LowCardinalityTags.MESSAGING_SYSTEM.withValue("kafka"));
        keyValues.add(context.operationNameValue(LowCardinalityTags.MESSAGING_OPERATION_NAME));
        keyValues.add(context.operationTypeValue(LowCardinalityTags.MESSAGING_OPERATION_TYPE));
        context.clientIdValue(LowCardinalityTags.MESSAGING_CLIENT_ID).ifPresent(keyValues::add);
        keyValues.add(context.topicValue(LowCardinalityTags.MESSAGING_DESTINATION_NAME));
        keyValues.add(context.partitionValue(LowCardinalityTags.MESSAGING_DESTINATION_PARTITION_ID));
        return KeyValues.of(keyValues);
    }

    @Override
    default KeyValues getHighCardinalityKeyValues(KafkaConsumeContext context) {
        return KeyValues.of(context.offsetValue(HighCardinalityTags.MESSAGING_KAFKA_OFFSET));
    }
}
