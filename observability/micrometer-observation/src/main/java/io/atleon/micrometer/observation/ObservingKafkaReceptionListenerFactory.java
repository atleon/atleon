package io.atleon.micrometer.observation;

import io.atleon.kafka.ReceptionListener;
import io.atleon.kafka.ReceptionListenerFactory;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.Observations;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collections;
import java.util.Map;

/**
 * A {@link ReceptionListener} that exports {@link KafkaObservations#POLLED} observations for every
 * {@link ConsumerRecord} received from {@link org.apache.kafka.clients.consumer.Consumer#poll(java.time.Duration)}
 */
public final class ObservingKafkaReceptionListenerFactory implements ReceptionListenerFactory {

    private final ObservationRegistry registry;

    private final Map<String, ?> consumerProperties;

    public ObservingKafkaReceptionListenerFactory() {
        this(Observations.getGlobalRegistry(), Collections.emptyMap());
    }

    public ObservingKafkaReceptionListenerFactory(ObservationRegistry registry) {
        this(registry, Collections.emptyMap());
    }

    private ObservingKafkaReceptionListenerFactory(ObservationRegistry registry, Map<String, ?> consumerProperties) {
        this.registry = registry;
        this.consumerProperties = consumerProperties;
    }

    @Override
    public ReceptionListenerFactory withConsumerProperties(Map<String, ?> consumerProperties) {
        return new ObservingKafkaReceptionListenerFactory(registry, consumerProperties);
    }

    @Override
    public ReceptionListener create() {
        return new Listener(KafkaConsumeContext.newFactory().withConsumerProperties(consumerProperties));
    }

    private final class Listener implements ReceptionListener {

        private final KafkaConsumeContext.Factory contextFactory;

        public Listener(KafkaConsumeContext.Factory contextFactory) {
            this.contextFactory = contextFactory;
        }

        @Override
        public void onRecordPolled(ConsumerRecord<?, ?> consumerRecord) {
            KafkaObservations.polled(registry, contextFactory, consumerRecord)
                    .start()
                    .stop();
        }
    }
}
