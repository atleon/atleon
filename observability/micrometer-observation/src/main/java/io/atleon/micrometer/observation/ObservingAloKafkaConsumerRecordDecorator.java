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
        extends ObservingAloConsumptionDecorator<ConsumerRecord<K, V>, KafkaConsumeContext>
        implements AloKafkaConsumerRecordDecorator<K, V> {

    private KafkaConsumeContext.Factory contextFactory;

    public ObservingAloKafkaConsumerRecordDecorator() {
        super(KafkaObservations.PROCESS);
        this.contextFactory = KafkaConsumeContext.newFactory();
    }

    public ObservingAloKafkaConsumerRecordDecorator(ObservationRegistry registry) {
        this(registry, KafkaConsumeContext.newFactory());
    }

    public ObservingAloKafkaConsumerRecordDecorator(
            ObservationRegistry registry, KafkaConsumeContext.Factory contextFactory) {
        super(KafkaObservations.PROCESS, registry);
        this.contextFactory = contextFactory;
    }

    @Override
    public void configure(Map<String, ?> properties) {
        this.contextFactory = contextFactory.withConsumerProperties(properties);
    }

    @Override
    protected ObservationConvention<KafkaConsumeContext> defaultConvention() {
        return KafkaProcessObservationConvention.Default.INSTANCE;
    }

    @Override
    protected KafkaConsumeContext createContext(ConsumerRecord<K, V> consumerRecord) {
        return contextFactory.create(consumerRecord);
    }
}
