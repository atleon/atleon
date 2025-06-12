package io.atleon.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReceivingConsumerTest {

    @Test
    public void invoke_givenPermittedMethodCallsToPauseAndResume_expectsReturnedResult() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
            .consumerProperties(Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, "test"))
            .build();

        Set<TopicPartition> paused = new HashSet<>();
        Set<TopicPartition> resumed = new HashSet<>();
        NoOpPartitioningListener noOpPartitioningListener = new NoOpPartitioningListener() {
            @Override
            public void onPartitionsExternallyPaused(Collection<TopicPartition> partitions) {
                paused.addAll(partitions);
            }

            @Override
            public void onPartitionsExternallyResumed(Collection<TopicPartition> partitions) {
                resumed.addAll(partitions);
            }
        };

        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
            new ReceivingConsumer<>(options, noOpPartitioningListener, error::tryEmitValue);
        mockConsumer.assign(Collections.singletonList(topicPartition));

        receivingConsumer.invoke(consumer -> consumer.pause(Collections.singletonList(topicPartition))).block();
        receivingConsumer.invoke(consumer -> consumer.resume(Collections.singletonList(topicPartition))).block();

        assertEquals(Collections.singleton(topicPartition), paused);
        assertEquals(Collections.singleton(topicPartition), resumed);
    }

    @Test
    public void invoke_givenProhibitedMethodCall_expectsUnsupportedOperationException() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
            .consumerProperties(Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, "test"))
            .build();
        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
            new ReceivingConsumer<>(options, new NoOpPartitioningListener(), error::tryEmitValue);

        assertThrows(UnsupportedOperationException.class, receivingConsumer.invoke(Consumer::wakeup)::block);
    }

    @Test
    public void invokeAndGet_givenCallFromPollingThread_expectsUnsupportedOperationException() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
            .consumerProperties(Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, "test"))
            .build();
        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
            new ReceivingConsumer<>(options, new NoOpPartitioningListener(), error::tryEmitValue);

        receivingConsumer.schedule(__ -> receivingConsumer.invokeAndGet(Consumer::paused));

        assertInstanceOf(UnsupportedOperationException.class, error.asMono().block());
    }

    @Test
    public void onPartitionsAssigned_givenCallOnProxyFromNonPollingThread_expectsUnsupportedOperationException() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        ReceptionListener receptionListener = new ReceptionListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                consumer.paused();
            }
        };

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
            .listener(receptionListener)
            .consumerProperties(Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, "test"))
            .build();
        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
            new ReceivingConsumer<>(options, new NoOpPartitioningListener(), error::tryEmitValue);

        assertThrows(
            UnsupportedOperationException.class,
            () -> receivingConsumer.onPartitionsAssigned(Collections.emptyList()));
    }

    @Test
    public void subscribe_givenSubscriptionError_expectsContinuationOfTaskLoop() {
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
            .consumerProperties(Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, "test"))
            .build();
        Sinks.One<Throwable> error = Sinks.one();

        ReceivingConsumer<String, String> receivingConsumer =
            new ReceivingConsumer<>(options, new NoOpPartitioningListener(), error::tryEmitValue);

        receivingConsumer.subscribe(
            (consumer, listener) -> consumer.subscribe(Collections.singletonList("topic")),
            consumer -> {
                throw new UnsupportedOperationException("Bang");
            });

        receivingConsumer.safelyClose(Duration.ofSeconds(5)).block();

        assertInstanceOf(UnsupportedOperationException.class, error.asMono().block());
        assertTrue(mockConsumer.closed());
    }

    public class NoOpPartitioningListener implements ReceivingConsumer.PartitioningListener {

        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {

        }

        @Override
        public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {

        }

        @Override
        public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {

        }

        @Override
        public void onPartitionsExternallyPaused(Collection<TopicPartition> partitions) {

        }

        @Override
        public void onPartitionsExternallyResumed(Collection<TopicPartition> partitions) {

        }
    }
}