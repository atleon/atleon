package io.atleon.micrometer.observation;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.docs.ObservationDocumentation;

import java.util.function.Supplier;

/**
 * Observations for reactive Kafka operations. Note that tag names are based on published
 * <a href="https://opentelemetry.io/docs/specs/semconv/messaging/kafka">semantic conventions</a>.
 */
public enum KafkaObservations implements ObservationDocumentation {
    /**
     * Observation for records received from polling
     */
    POLLED(KafkaPolledObservationConvention.Default.class),
    /**
     * Observation for processing consumed records
     */
    PROCESS(KafkaProcessObservationConvention.Default.class);

    private final Class<? extends ObservationConvention<? extends Observation.Context>> defaultConvention;

    KafkaObservations(Class<? extends ObservationConvention<? extends Observation.Context>> defaultConvention) {
        this.defaultConvention = defaultConvention;
    }

    /**
     * Convenience function for creating a "polled" {@link Observation} with default convention
     */
    public static Observation polled(Supplier<KafkaConsumeContext> contextSupplier, ObservationRegistry registry) {
        return POLLED.observation(null, KafkaPolledObservationConvention.Default.INSTANCE, contextSupplier, registry);
    }

    /**
     * Convenience function for creating a "process" {@link Observation} with default convention
     */
    public static Observation process(Supplier<KafkaConsumeContext> contextSupplier, ObservationRegistry registry) {
        return PROCESS.observation(null, KafkaProcessObservationConvention.Default.INSTANCE, contextSupplier, registry);
    }

    @Override
    public Class<? extends ObservationConvention<? extends Observation.Context>> getDefaultConvention() {
        return defaultConvention;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
        return LowCardinalityTags.values();
    }

    @Override
    public KeyName[] getHighCardinalityKeyNames() {
        return HighCardinalityTags.values();
    }

    public enum LowCardinalityTags implements KeyName {
        /**
         * Identifier of messaging system ("kafka" for these observations)
         */
        MESSAGING_SYSTEM("messaging.system"),
        /**
         * The name of operation being applied (e.g. "polled", "process", "send", etc.)
         */
        MESSAGING_OPERATION_NAME("messaging.operation.name"),
        /**
         * The type of operation being applied (e.g. "receive", "process", "send", etc.)
         */
        MESSAGING_OPERATION_TYPE("messaging.operation.type"),
        /**
         * ID of underlying Kafka client
         */
        MESSAGING_CLIENT_ID("messaging.client.id"),
        /**
         * Topic through which record is consumed/produced
         */
        MESSAGING_DESTINATION_NAME("messaging.destination.name"),
        /**
         * Partition of topic through which record is consumed/produced
         */
        MESSAGING_DESTINATION_PARTITION_ID("messaging.destination.partition.id");

        private final String key;

        LowCardinalityTags(String key) {
            this.key = key;
        }

        @Override
        public String asString() {
            return key;
        }
    }

    public enum HighCardinalityTags implements KeyName {
        /**
         * Offset in topic-partition from which consumer record is received
         */
        MESSAGING_KAFKA_OFFSET("messaging.kafka.offset");

        private final String key;

        HighCardinalityTags(String key) {
            this.key = key;
        }

        @Override
        public String asString() {
            return key;
        }
    }
}
