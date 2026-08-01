package io.atleon.micrometer.observation;

import io.micrometer.observation.tck.TestObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;

class ObservingKafkaReceptionListenerTest {

    private final TestObservationRegistry registry = TestObservationRegistry.create();

    @Test
    public void onRecordPolled_givenConsumerRecord_expectsStoppedObservationTaggedFromRecord() {
        ObservingKafkaReceptionListener listener = new ObservingKafkaReceptionListener(registry);

        listener.onRecordPolled(new ConsumerRecord<>("topic", 2, 42L, "key", "value"));

        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("atleon.kafka.polled")
                .that()
                .hasBeenStopped()
                .doesNotHaveError()
                .hasLowCardinalityKeyValue("messaging.system", "kafka")
                .hasLowCardinalityKeyValue("messaging.operation.name", "polled")
                .hasLowCardinalityKeyValue("messaging.operation.type", "receive")
                .doesNotHaveLowCardinalityKeyValueWithKey("messaging.client.id")
                .hasLowCardinalityKeyValue("messaging.destination.name", "topic")
                .hasLowCardinalityKeyValue("messaging.destination.partition.id", "2")
                .hasHighCardinalityKeyValue("messaging.kafka.offset", "42");
    }

    @Test
    public void onRecordPolled_givenContextFactoryWithClientId_expectsObservationTaggedWithClientId() {
        ObservingKafkaReceptionListener listener = new ObservingKafkaReceptionListener(
                registry, KafkaConsumeContext.newFactory().withClientId("test-client"));

        listener.onRecordPolled(new ConsumerRecord<>("topic", 0, 0L, "key", "value"));

        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("atleon.kafka.polled")
                .that()
                .hasBeenStopped()
                .hasLowCardinalityKeyValue("messaging.client.id", "test-client");
    }
}
