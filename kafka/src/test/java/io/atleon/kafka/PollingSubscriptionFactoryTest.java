package io.atleon.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PollingSubscriptionFactoryTest {

    @Test
    public void poll_givenInsufficientPrefetchCapacity_expectsPausing() {
        String topic = "topic";
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(new TopicPartition(topic, 0), 0L);
        Sinks.Many<Long> polled = Sinks.many().multicast().directBestEffort();

        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(beginningOffsets.keySet()));
        schedulePollEventing(mockConsumer, polled);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
            .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
            .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
            .fullPollRecordsPrefetch(1)
            .build();

        KafkaReceiver.create(options)
            .receiveManual(Collections.singletonList(topic))
            .as(it -> StepVerifier.create(it, 1))
            .then(polled.asFlux().take(5).then()::block)
            .then(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "value")))
            .then(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 1L, "key", "value")))
            .expectNextCount(1L)
            .then(polled.asFlux().take(5).then()::block)
            .then(() -> assertEquals(beginningOffsets.keySet(), mockConsumer.paused()))
            .thenRequest(1L)
            .expectNextCount(1)
            .then(polled.asFlux().take(5).then()::block)
            .then(() -> assertTrue(mockConsumer.paused().isEmpty()))
            .thenCancel()
            .verify();
    }

    @Test
    public void poll_givenExternallyPausedPartitions_expectsAppropriatePausingAndResuming() {
        String topic = "topic";
        TopicPartition firstTopicPartition = new TopicPartition(topic, 0);
        TopicPartition secondTopicPartition = new TopicPartition(topic, 1);
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(firstTopicPartition, 0L);
        beginningOffsets.put(secondTopicPartition, 0L);
        Sinks.Many<Long> polled = Sinks.many().multicast().directBestEffort();

        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(beginningOffsets.keySet()));
        schedulePollEventing(mockConsumer, polled);

        ConsumerListener consumerListener = ConsumerListener.doOnPartitionsAssignedOnce((consumer, partitions) -> {
            if (partitions.contains(firstTopicPartition)) {
                consumer.pause(Collections.singletonList(firstTopicPartition));
            }
        });
        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
            .consumerListener(consumerListener)
            .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
            .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
            .fullPollRecordsPrefetch(1)
            .build();

        KafkaReceiver.create(options)
            .receiveManual(Collections.singletonList(topic))
            .as(it -> StepVerifier.create(it, 1))
            .then(polled.asFlux().take(5).then()::block)
            .then(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "value")))
            .then(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 1, 0L, "key", "value")))
            .then(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 1L, "key", "value")))
            .then(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 1, 1L, "key", "value")))
            .expectNextMatches(it -> it.topicPartition().equals(secondTopicPartition))
            .then(polled.asFlux().take(5).then()::block)
            .then(() -> assertEquals(beginningOffsets.keySet(), mockConsumer.paused()))
            .thenRequest(1L)
            .expectNextCount(1)
            .then(polled.asFlux().take(5).then()::block)
            .then(() -> assertEquals(Collections.singleton(firstTopicPartition), mockConsumer.paused()))
            .thenCancel()
            .verify();
    }

    @Test
    public void commit_givenNonRetriableFailure_expectsError() {
        String topic = "topic";
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(new TopicPartition(topic, 0), 0L);
        Sinks.Many<Long> polled = Sinks.many().multicast().directBestEffort();

        MockConsumer<String, String> mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
                callback.onComplete(offsets, new UnsupportedOperationException("Boom"));
            }
        };
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(beginningOffsets.keySet()));
        schedulePollEventing(mockConsumer, polled);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
            .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
            .commitBatchSize(1)
            .build();

        KafkaReceiver.create(options)
            .receiveManual(Collections.singletonList(topic))
            .as(it -> StepVerifier.create(it, 1))
            .then(polled.asFlux().take(5).then()::block)
            .then(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "value")))
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .expectError(UnsupportedOperationException.class)
            .verify();
    }

    @Test
    public void commit_givenRetriableFailureWithEventualSuccess_expectsRetryAndSuccess() {
        String topic = "topic";
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(new TopicPartition(topic, 0), 0L);
        Sinks.Many<Long> polled = Sinks.many().multicast().directBestEffort();

        AtomicInteger commitAttempts = new AtomicInteger(0);
        Sinks.Empty<Void> succeeded = Sinks.empty();
        MockConsumer<String, String> mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
                int attempt = commitAttempts.incrementAndGet();
                if (attempt == 1) {
                    // First attempt fails with retriable error
                    callback.onComplete(offsets, new RetriableCommitFailedException("Persistent network error"));
                } else {
                    // Subsequent attempts succeed
                    callback.onComplete(offsets, null);
                    succeeded.tryEmitEmpty();
                }
            }
        };
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(beginningOffsets.keySet()));
        schedulePollEventing(mockConsumer, polled);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
            .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
            .commitBatchSize(1)
            .maxCommitAttempts(3)
            .build();

        KafkaReceiver.create(options)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .then(polled.asFlux().take(5).then()::block)
            .then(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "value")))
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .then(polled.asFlux().take(5).then()::block)
            .thenCancel()
            .verify();

        // Verify retry occurred and eventually succeeded
        assertEquals(2, commitAttempts.get());
    }

    @Test
    public void commit_givenRetriableFailureWithRetriesExhausted_expectsError() {
        String topic = "topic";
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(new TopicPartition(topic, 0), 0L);
        Sinks.Many<Long> polled = Sinks.many().multicast().directBestEffort();

        AtomicInteger commitAttempts = new AtomicInteger(0);
        MockConsumer<String, String> mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
                commitAttempts.incrementAndGet();
                // Always fail with retriable error
                callback.onComplete(offsets, new RetriableCommitFailedException("Persistent network error"));
            }
        };
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(beginningOffsets.keySet()));
        schedulePollEventing(mockConsumer, polled);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
            .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
            .commitBatchSize(1)
            .maxCommitAttempts(2) // Only allow 2 attempts
            .build();

        KafkaReceiver.create(options)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .then(polled.asFlux().take(5).then()::block)
            .then(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "value")))
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .expectError(KafkaException.class)
            .verify();

        // Verify max attempts were made
        assertEquals(options.maxCommitAttempts(), commitAttempts.get());
    }

    private static void schedulePollEventing(MockConsumer<String, String> mockConsumer, Sinks.Many<Long> polled) {
        mockConsumer.schedulePollTask(() -> {
            polled.tryEmitNext(System.currentTimeMillis());
            schedulePollEventing(mockConsumer, polled);
        });
    }
}