package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumerMutexEnforcerTest {

    @Test
    public void newConsumerFactory_givenDetectionOfOrphanedConsumer_expectsIllegalStateOnOldConsumer() {
        ConsumerMutexEnforcer enforcer = new TestConsumerMutexEnforcer();

        Consumer<Object, Object> oldConsumer = enforcer.newConsumerFactory().apply(Collections.emptyMap());
        Consumer<Object, Object> newConsumer = enforcer.newConsumerFactory().apply(Collections.emptyMap());

        assertThrows(IllegalStateException.class, () -> oldConsumer.poll(Duration.ZERO));
        assertDoesNotThrow(() -> newConsumer.poll(Duration.ZERO));
    }

    private static final class TestConsumerMutexEnforcer extends ConsumerMutexEnforcer {

        @Override
        protected <K, V> Consumer<K, V> newConsumer(Map<String, Object> config) {
            Consumer<K, V> consumer = mock(Consumer.class);
            when(consumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
            return consumer;
        }
    }
}
