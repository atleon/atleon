package io.atleon.micrometer.observation;

import io.atleon.core.Alo;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ObservingAloKafkaConsumerRecordDecoratorTest {

    private final TestObservationRegistry registry = TestObservationRegistry.create();

    private final ObservingAloKafkaConsumerRecordDecorator<String, String> decorator =
            new ObservingAloKafkaConsumerRecordDecorator<>(registry);

    @Test
    public void decorate_givenConfiguredClientId_expectsObservationTaggedFromConsumerRecord() {
        decorator.configure(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, "test-client"));

        TestAlo<ConsumerRecord<String, String>> alo =
                new TestAlo<>(new ConsumerRecord<>("topic", 2, 42L, "key", "value"));
        Alo<ConsumerRecord<String, String>> decorated = decorator.decorate(alo);

        // Decoration starts the observation before the record is acknowledged
        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("atleon.kafka.process")
                .that()
                .hasBeenStarted()
                .isNotStopped();

        Alo.acknowledge(decorated);

        assertEquals(1, alo.acknowledgeCount());
        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("atleon.kafka.process")
                .that()
                .hasBeenStopped()
                .doesNotHaveError()
                .hasLowCardinalityKeyValue("messaging.system", "kafka")
                .hasLowCardinalityKeyValue("messaging.operation.type", "process")
                .hasLowCardinalityKeyValue("messaging.client.id", "test-client")
                .hasLowCardinalityKeyValue("messaging.destination.name", "topic")
                .hasLowCardinalityKeyValue("messaging.destination.partition.id", "2")
                .hasHighCardinalityKeyValue("messaging.kafka.offset", "42");
    }

    @Test
    public void decorate_givenNoConfiguredClientId_expectsObservationWithoutClientIdTag() {
        decorator.configure(Collections.emptyMap());

        TestAlo<ConsumerRecord<String, String>> alo =
                new TestAlo<>(new ConsumerRecord<>("topic", 0, 0L, "key", "value"));
        Alo.acknowledge(decorator.decorate(alo));

        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("atleon.kafka.process")
                .that()
                .hasBeenStopped()
                .doesNotHaveLowCardinalityKeyValueWithKey("messaging.client.id")
                .hasLowCardinalityKeyValue("messaging.destination.name", "topic")
                .hasLowCardinalityKeyValue("messaging.destination.partition.id", "0")
                .hasHighCardinalityKeyValue("messaging.kafka.offset", "0");
    }

    @Test
    public void decorate_givenNacknowledgedRecord_expectsErrorRecordedOnObservation() {
        decorator.configure(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, "test-client"));

        TestAlo<ConsumerRecord<String, String>> alo =
                new TestAlo<>(new ConsumerRecord<>("topic", 0, 0L, "key", "value"));

        RuntimeException error = new IllegalStateException("Boom");
        Alo.nacknowledge(decorator.decorate(alo), error);

        assertEquals(0, alo.acknowledgeCount());
        assertEquals(1, alo.nacknowledgeCount());
        assertThat(registry)
                .hasNumberOfObservationsEqualTo(1)
                .hasObservationWithNameEqualTo("atleon.kafka.process")
                .that()
                .hasBeenStopped()
                .hasError(error);
    }
}
