package io.atleon.micrometer.observation;

import io.atleon.kafka.ReceptionListener;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.Observations;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;

/**
 * A {@link ReceptionListener} that exports {@link KafkaObservations#POLLED} observations for every
 * {@link ConsumerRecord} received from {@link org.apache.kafka.clients.consumer.Consumer#poll(Duration)}
 */
public final class ObservingKafkaReceptionListener implements ReceptionListener {

    private final ObservationRegistry registry;

    private final KafkaConsumeContext.Factory contextFactory;

    public ObservingKafkaReceptionListener() {
        this(Observations.getGlobalRegistry(), KafkaConsumeContext.newFactory());
    }

    public ObservingKafkaReceptionListener(ObservationRegistry registry) {
        this(registry, KafkaConsumeContext.newFactory());
    }

    public ObservingKafkaReceptionListener(ObservationRegistry registry, KafkaConsumeContext.Factory contextFactory) {
        this.registry = registry;
        this.contextFactory = contextFactory;
    }

    @Override
    public void onRecordPolled(ConsumerRecord<?, ?> consumerRecord) {
        KafkaObservations.polled(() -> contextFactory.polled(consumerRecord), registry)
                .start()
                .stop();
    }
}
