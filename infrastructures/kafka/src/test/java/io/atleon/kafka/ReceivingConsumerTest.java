package io.atleon.kafka;

import io.atleon.util.Throwing;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReceivingConsumerTest {

    @Test
    public void invoke_givenPermittedMethodCallsToPauseAndResume_expectsReturnedResult() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();

        Set<TopicPartition> paused = new HashSet<>();
        Set<TopicPartition> resumed = new HashSet<>();
        NoOpPartitionListener partitionListener = new NoOpPartitionListener() {
            @Override
            public void onExternalPartitionsPauseRequested(Collection<TopicPartition> partitions) {
                paused.addAll(partitions);
            }

            @Override
            public void onExternalPartitionsResumeRequested(Collection<TopicPartition> partitions) {
                resumed.addAll(partitions);
            }
        };

        ReceivingConsumer<String, String> receivingConsumer =
                new ReceivingConsumer<>(options, partitionListener, this::throwPropagated);
        mockConsumer.assign(Collections.singletonList(topicPartition));

        receivingConsumer
                .invoke(consumer -> consumer.pause(Collections.singletonList(topicPartition)))
                .block();
        receivingConsumer
                .invoke(consumer -> consumer.resume(Collections.singletonList(topicPartition)))
                .block();

        assertEquals(Collections.singleton(topicPartition), paused);
        assertEquals(Collections.singleton(topicPartition), resumed);
    }

    @Test
    public void invoke_givenPermittedPauseAndResume_expectsCorrectPausing() {
        TopicPartition topicPartition1 = new TopicPartition("topic", 0);
        TopicPartition topicPartition2 = new TopicPartition("topic", 1);
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();

        NoOpPartitionListener partitionListener = new NoOpPartitionListener() {

            private final Set<TopicPartition> paused = new HashSet<>();

            @Override
            public void onExternalPartitionsPauseRequested(Collection<TopicPartition> partitions) {
                paused.addAll(partitions);
            }

            @Override
            public void onExternalPartitionsResumeRequested(Collection<TopicPartition> partitions) {
                paused.removeAll(partitions);
            }

            @Override
            public Collection<TopicPartition> grantedPartitionPauses() {
                return paused;
            }
        };

        ReceivingConsumer<String, String> receivingConsumer =
                new ReceivingConsumer<>(options, partitionListener, this::throwPropagated);

        Mono<Void> pauseResume = receivingConsumer.invoke(it -> {
            it.pause(Collections.singletonList(topicPartition1));
            it.pause(Collections.singletonList(topicPartition2));
            it.resume(Collections.singletonList(topicPartition1));
        });
        pauseResume.block();

        Set<TopicPartition> paused =
                receivingConsumer.invokeAndGet(Consumer::paused).block();

        assertEquals(Collections.singleton(topicPartition2), paused);
    }

    @Test
    public void invoke_givenProhibitedMethodCall_expectsUnsupportedOperationException() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();
        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
                new ReceivingConsumer<>(options, new NoOpPartitionListener(), error::tryEmitValue);

        assertThrows(UnsupportedOperationException.class, receivingConsumer.invoke(Consumer::wakeup)::block);
    }

    @Test
    public void invokeAndGet_givenCallFromPollingThread_expectsUnsupportedOperationException() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();
        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
                new ReceivingConsumer<>(options, new NoOpPartitionListener(), error::tryEmitValue);

        receivingConsumer.schedule(__ -> receivingConsumer.invokeAndGet(Consumer::paused));

        assertInstanceOf(UnsupportedOperationException.class, error.asMono().block());
    }

    @Test
    public void onPartitionsAssigned_givenCallOnProxyFromNonPollingThread_expectsUnsupportedOperationException() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        ConsumerListener consumerListener = new ConsumerListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                consumer.paused();
            }
        };

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerListener(consumerListener)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();
        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
                new ReceivingConsumer<>(options, new NoOpPartitionListener(), error::tryEmitValue);

        assertThrows(
                UnsupportedOperationException.class,
                () -> receivingConsumer.onPartitionsAssigned(Collections.emptyList()));
    }

    @Test
    public void init_givenConsumptionError_expectsContinuationOfTaskLoop() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();
        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
                new ReceivingConsumer<>(options, new NoOpPartitionListener(), error::tryEmitValue);

        ConsumptionSpec consumptionSpec = ConsumptionSpec.subscribe(Collections.singletonList("topic"));
        receivingConsumer.init(consumptionSpec, consumer -> {
            throw new UnsupportedOperationException("Bang");
        });

        receivingConsumer.closeSafely(consumptionSpec).block();

        assertInstanceOf(UnsupportedOperationException.class, error.asMono().block());
        assertTrue(mockConsumer.closed());
    }

    @Test
    public void wakeupSafely_givenInvocationFromTaskThread_expectsNoOp() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();
        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
                new ReceivingConsumer<>(options, new NoOpPartitionListener(), error::tryEmitValue);

        receivingConsumer.schedule(__ -> {
            receivingConsumer.wakeupSafely();
            error.tryEmitEmpty();
        });

        assertNull(error.asMono().block());
        assertDoesNotThrow(() -> mockConsumer.poll(Duration.ZERO));
    }

    @Test
    public void wakeupSafely_givenInvocationFromNonTaskThread_expectsNoOp() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .build();
        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
                new ReceivingConsumer<>(options, new NoOpPartitionListener(), error::tryEmitValue);

        receivingConsumer.wakeupSafely();

        assertThrows(WakeupException.class, () -> mockConsumer.poll(Duration.ZERO));
    }

    private void throwPropagated(Throwable error) {
        throw Throwing.propagate(error);
    }

    public static class NoOpPartitionListener implements ReceivingConsumer.PartitionListener {

        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}

        @Override
        public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}

        @Override
        public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {}

        @Override
        public void onExternalPartitionsPauseRequested(Collection<TopicPartition> partitions) {}

        @Override
        public void onExternalPartitionsResumeRequested(Collection<TopicPartition> partitions) {}

        @Override
        public Collection<TopicPartition> grantedPartitionPauses() {
            return Collections.emptyList();
        }
    }
}
