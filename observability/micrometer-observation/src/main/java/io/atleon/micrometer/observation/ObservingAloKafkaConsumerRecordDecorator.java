package io.atleon.micrometer.observation;

import io.atleon.core.Alo;
import io.atleon.kafka.AloKafkaConsumerRecordDecorator;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

/**
 * An {@link AloKafkaConsumerRecordDecorator} that decorates {@link Alo} elements with observations
 * derived from context extracted from {@link ConsumerRecord}s
 *
 * @param <K> The types of keys in records decorated by this decorator
 * @param <V> The types of values in records decorated by this decorator
 */
public final class ObservingAloKafkaConsumerRecordDecorator<K, V>
        extends ObservingAloConsumptionDecorator<ConsumerRecord<K, V>, KafkaReceiverContext>
        implements AloKafkaConsumerRecordDecorator<K, V> {

    private KafkaReceiverContext.Factory contextFactory;

    public ObservingAloKafkaConsumerRecordDecorator() {
        super(KafkaObservation.RECEIVER_OBSERVATION);
        this.contextFactory = KafkaReceiverContext.newFactory();
    }

    public ObservingAloKafkaConsumerRecordDecorator(ObservationRegistry registry) {
        this(registry, KafkaReceiverContext.newFactory());
    }

    public ObservingAloKafkaConsumerRecordDecorator(
            ObservationRegistry registry, KafkaReceiverContext.Factory contextFactory) {
        super(KafkaObservation.RECEIVER_OBSERVATION, registry);
        this.contextFactory = contextFactory;
    }

    @Override
    public void configure(Map<String, ?> properties) {
        this.contextFactory = contextFactory.withConsumerProperties(properties);
    }

    @Override
    protected ObservationConvention<KafkaReceiverContext> defaultConvention() {
        return KafkaReceiverObservationConvention.Default.instance();
    }

    @Override
    protected KafkaReceiverContext createContext(ConsumerRecord<K, V> consumerRecord) {
        return contextFactory.create(consumerRecord);
    }
}
