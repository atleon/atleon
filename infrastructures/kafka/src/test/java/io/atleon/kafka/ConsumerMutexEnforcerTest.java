package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumerMutexEnforcerTest {

    @Test
    public void consumptionIsProhibitedWhenOrphanedConsumerIsDetected() {
        ConsumerMutexEnforcer enforcer = new TestConsumerMutexEnforcer();

        Consumer<Object, Object> oldConsumer = enforcer.newConsumerFactory().createConsumer(ReceiverOptions.create());
        Consumer<Object, Object> newConsumer = enforcer.newConsumerFactory().createConsumer(ReceiverOptions.create());

        assertThrows(IllegalStateException.class, () -> oldConsumer.poll(Duration.ZERO));
        assertDoesNotThrow(() -> newConsumer.poll(Duration.ZERO));
    }

    @Test
    public void consumptionIsProhibitedAfterGracePeriod() {
        ConsumerMutexEnforcer enforcer = new TestConsumerMutexEnforcer();
        ConsumerMutexEnforcer.ProhibitableConsumerFactory factory = enforcer.newConsumerFactory();

        Consumer<Object, Object> consumer = factory.createConsumer(ReceiverOptions.create());
        factory.prohibitFurtherConsumption(Duration.ofNanos(-1));

        assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
    }

    private static final class TestConsumerMutexEnforcer extends ConsumerMutexEnforcer {

        @Override
        protected <K, V> Consumer<K, V> newConsumer(ReceiverOptions<K, V> options) {
            Consumer<K, V> consumer = mock(Consumer.class);
            when(consumer.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));
            return consumer;
        }
    }
}