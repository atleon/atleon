package io.atleon.micrometer.observation;

import io.micrometer.observation.tck.TestObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;

class ObservingKafkaReceptionListenerFactoryTest {

    private final TestObservationRegistry registry = TestObservationRegistry.create();

    @Test
    public void onRecordPolled_givenConsumerRecord_expectsStoppedObservationTaggedFromRecord() {
        new ObservingKafkaReceptionListenerFactory(registry)
                .create()
                .onRecordPolled(new ConsumerRecord<>("topic", 2, 42L, "key", "value"));

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
    public void onRecordPolled_givenConsumerPropertiesWithClientId_expectsObservationTaggedWithClientId() {
        new ObservingKafkaReceptionListenerFactory(registry)
                .withConsumerProperties(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, "test-client"))
                .create()
                .onRecordPolled(new ConsumerRecord<>("topic", 0, 0L, "key", "value"));

        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("atleon.kafka.polled")
                .that()
                .hasBeenStopped()
                .hasLowCardinalityKeyValue("messaging.client.id", "test-client");
    }
}
