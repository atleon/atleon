package io.atleon.micrometer.observation;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.docs.ObservationDocumentation;

import java.util.function.Supplier;

/**
 * Observations for reactive Kafka operations
 */
public enum KafkaObservation implements ObservationDocumentation {
    /**
     * Observation for each received record from Kafka
     */
    RECEIVER_OBSERVATION {
        @Override
        public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
            return KafkaReceiverObservationConvention.Default.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return ReceiverLowCardinalityTags.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return ReceiverHighCardinalityTags.values();
        }
    };

    /**
     * Convenience function for creating an {@link Observation} with default convention
     */
    public static Observation receiverObservation(
            Supplier<KafkaReceiverContext> contextSupplier, ObservationRegistry registry) {
        return RECEIVER_OBSERVATION.observation(
                null, KafkaReceiverObservationConvention.Default.instance(), contextSupplier, registry);
    }

    public enum ReceiverLowCardinalityTags implements KeyName {
        /**
         * Client ID of receiver's underlying consumer
         */
        CLIENT_ID("client_id"),
        /**
         * Topic from which consumer record is received
         */
        TOPIC("topic"),
        /**
         * Partition of topic from which consumer record is received
         */
        PARTITION("partition");

        private final String key;

        ReceiverLowCardinalityTags(String key) {
            this.key = key;
        }

        @Override
        public String asString() {
            return key;
        }
    }

    public enum ReceiverHighCardinalityTags implements KeyName {
        /**
         * Offset in topic-partition from which consumer record is received
         */
        OFFSET("offset");

        private final String key;

        ReceiverHighCardinalityTags(String key) {
            this.key = key;
        }

        @Override
        public String asString() {
            return key;
        }
    }
}
