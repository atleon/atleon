package io.atleon.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    public void poll_givenExhaustedActiveInFlightCapacity_expectsEmissionPaused() {
        String topic = "topic";
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(new TopicPartition(topic, 0), 0L);
        Sinks.Many<Long> polled = Sinks.many().multicast().directBestEffort();

        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(beginningOffsets.keySet()));
        schedulePollEventing(mockConsumer, polled);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2)
                .fullPollRecordsPrefetch(1)
                .maxActiveInFlight(1L)
                .build();

        AtomicReference<KafkaReceiverRecord<String, String>> firstReceived = new AtomicReference<>();
        KafkaReceiver.create(options)
                .receiveManual(Collections.singletonList(topic))
                .as(StepVerifier::create)
                .then(polled.asFlux().take(5).then()::block)
                .then(() -> {
                    mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "first"));
                    mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 1L, "key", "second"));
                })
                .consumeNextWith(firstReceived::set)
                .expectNoEvent(Duration.ofMillis(100L))
                .then(() -> firstReceived.get().acknowledge())
                .consumeNextWith(KafkaReceiverRecord::acknowledge)
                .thenCancel()
                .verify();
    }

    @Test
    public void poll_givenSkippedRecord_expectsActiveInFlightCapacityToRemainBounded() {
        String topic = "topic";
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(new TopicPartition(topic, 0), 0L);
        Sinks.Many<Long> polled = Sinks.many().multicast().directBestEffort();

        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(beginningOffsets.keySet()));
        schedulePollEventing(mockConsumer, polled);

        AtomicLong activated = new AtomicLong();
        AtomicLong deactivated = new AtomicLong();
        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .receptionListener(new ReceptionListener() {
                    @Override
                    public void onRecordsActivated(TopicPartition partition, long count) {
                        activated.addAndGet(count);
                    }

                    @Override
                    public void onRecordsDeactivated(TopicPartition partition, long count) {
                        deactivated.addAndGet(count);
                    }
                })
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3)
                .fullPollRecordsPrefetch(1)
                .maxActiveInFlight(1L)
                .offsetTrackingStrategy(skippingOffsetTrackingStrategy(0L))
                .build();

        AtomicReference<KafkaReceiverRecord<String, String>> firstReceived = new AtomicReference<>();
        KafkaReceiver.create(options)
                .receiveManual(Collections.singletonList(topic))
                .as(StepVerifier::create)
                .then(polled.asFlux().take(5).then()::block)
                .then(() -> {
                    mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "skipped"));
                    mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 1L, "key", "first"));
                    mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 2L, "key", "second"));
                })
                .consumeNextWith(it -> {
                    assertEquals(1L, it.consumerRecord().offset());
                    firstReceived.set(it);
                })
                .expectNoEvent(Duration.ofMillis(100L))
                .then(() -> firstReceived.get().acknowledge())
                .consumeNextWith(it -> {
                    assertEquals(2L, it.consumerRecord().offset());
                    it.acknowledge();
                })
                .then(() -> {
                    assertEquals(3L, activated.get());
                    assertEquals(3L, deactivated.get());
                })
                .thenCancel()
                .verify();
    }

    @Test
    public void rebalance_givenNoConsumedRecords_expectsNoError() {
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
                .then(() -> mockConsumer.rebalance(Collections.emptyList()))
                .then(polled.asFlux().take(5).then()::block)
                .thenCancel()
                .verify();
    }

    @Test
    public void poll_givenGroupMetadataChangedWithoutRebalance_expectsRefreshedGroupMetadataSentInTransaction() {
        String topic = "topic";
        String groupId = "group";
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(new TopicPartition(topic, 0), 0L);
        Sinks.Many<Long> polled = Sinks.many().multicast().directBestEffort();

        CountDownLatch offsetsSent = new CountDownLatch(1);
        KafkaTxManager txManager = mock(KafkaTxManager.class);
        when(txManager.begin()).thenReturn(Mono.empty());
        when(txManager.commit()).thenReturn(Mono.empty());
        when(txManager.abort()).thenReturn(Mono.empty());
        when(txManager.sendOffsets(any(), any())).thenAnswer(__ -> {
            offsetsSent.countDown();
            return Mono.empty();
        });

        // Emulates a generation bump that retains this member's assignment, as can happen with
        // cooperative rebalancing. Note that no rebalance callback is invoked for such a bump, so
        // polling is the only opportunity to observe the updated metadata.
        AtomicInteger generation = new AtomicInteger(1);
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public ConsumerGroupMetadata groupMetadata() {
                return new ConsumerGroupMetadata(groupId, generation.get(), "member", Optional.empty());
            }
        };
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(beginningOffsets.keySet()));
        schedulePollEventing(mockConsumer, polled);

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
                .fullPollRecordsPrefetch(1)
                .commitBatchSize(1)
                .build();

        AtomicReference<KafkaReceiverRecord<String, String>> received = new AtomicReference<>();
        KafkaReceiver.create(options)
                .receiveTxManual(Mono.just(txManager), Collections.singletonList(topic))
                .as(it -> StepVerifier.create(it, 1))
                .then(polled.asFlux().take(5).then()::block)
                .then(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "value")))
                .consumeNextWith(received::set)
                .then(() -> generation.set(2))
                .then(polled.asFlux().take(5).then()::block)
                .then(() -> received.get().acknowledge())
                .then(() -> assertTrue(awaitLatch(offsetsSent)))
                .thenCancel()
                .verify();

        ArgumentCaptor<ConsumerGroupMetadata> metadata = ArgumentCaptor.forClass(ConsumerGroupMetadata.class);
        verify(txManager, times(1)).sendOffsets(any(), metadata.capture());
        assertEquals(groupId, metadata.getValue().groupId());
        assertEquals(2, metadata.getValue().generationId());
    }

    @Test
    public void poll_givenPartitionsLostWhileTransactionOpening_expectsErrorAndAbortWithoutEmission() {
        String topic = "topic";
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(new TopicPartition(topic, 0), 0L);

        AtomicReference<ConsumerRebalanceListener> rebalanceListener = new AtomicReference<>();
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
                rebalanceListener.set(listener);
                super.subscribe(topics, listener);
            }
        };
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(beginningOffsets.keySet());
            mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "value"));
        });

        Sinks.Empty<Void> transactionOpened = Sinks.empty();
        KafkaTxManager txManager = mock(KafkaTxManager.class);
        when(txManager.begin()).thenReturn(transactionOpened.asMono().doOnSubscribe(__ -> {
            // Keep begin pending until loss is reported on the polling thread, then allow its
            // completion to verify that the buffered record cannot escape the invalidated state.
            mockConsumer.schedulePollTask(() -> {
                rebalanceListener.get().onPartitionsLost(beginningOffsets.keySet());
                mockConsumer.assign(Collections.emptyList());
                transactionOpened.tryEmitEmpty();
            });
        }));
        // As with the producer task loop, abortion completes after the pending begin operation.
        when(txManager.abort()).thenReturn(transactionOpened.asMono());

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerListener(closureListener)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
                .fullPollRecordsPrefetch(1)
                .build();

        KafkaReceiver.create(options)
                .receiveTxManual(Mono.just(txManager), Collections.singletonList(topic))
                .as(it -> StepVerifier.create(it, 1))
                .expectErrorMatches(it -> it instanceof IllegalStateException
                        && it.getMessage().equals("Partitions lost during transactional reception"))
                .verify(Duration.ofSeconds(10L));

        closureListener.closed().block(Duration.ofSeconds(10L));
        verify(txManager).begin();
        verify(txManager).abort();
        verify(txManager, never()).sendOffsets(any(), any());
        verify(txManager, never()).commit();
    }

    @Test
    public void rebalance_givenBufferedRecordAndInactiveTransaction_expectsNoEmissionFromRevokedPartition()
            throws InterruptedException {
        String topic = "topic";
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(new TopicPartition(topic, 0), 0L);

        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(beginningOffsets.keySet());
            mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "first"));
            mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 1L, "key", "buffered"));
        });

        CountDownLatch transactionCommitted = new CountDownLatch(1);
        CountDownLatch partitionsRevoked = new CountDownLatch(1);
        CountDownLatch firstReceived = new CountDownLatch(1);
        CountDownLatch secondReceived = new CountDownLatch(1);
        KafkaTxManager txManager = mock(KafkaTxManager.class);
        when(txManager.begin()).thenReturn(Mono.empty());
        when(txManager.sendOffsets(any(), any())).thenReturn(Mono.empty());
        when(txManager.commit()).thenReturn(Mono.fromRunnable(transactionCommitted::countDown));
        when(txManager.abort()).thenReturn(Mono.empty());

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2)
                .fullPollRecordsPrefetch(1)
                .commitBatchSize(1)
                .build();

        AtomicInteger received = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Subscriber<KafkaReceiverRecord<String, String>> subscriber = new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {}

            @Override
            public void onNext(KafkaReceiverRecord<String, String> record) {
                record.acknowledge();
                if (received.getAndIncrement() == 0) {
                    firstReceived.countDown();
                } else {
                    secondReceived.countDown();
                }
            }

            @Override
            public void onError(Throwable error) {
                failure.set(error);
            }

            @Override
            public void onComplete() {}
        };
        Subscription subscription = new PollingSubscriptionFactory<>(options)
                .transactional(txManager, ConsumptionSpec.subscribe(Collections.singletonList(topic)), subscriber);

        try {
            subscription.request(1L);
            assertTrue(awaitLatch(firstReceived));
            assertTrue(awaitLatch(transactionCommitted));

            mockConsumer.schedulePollTask(() -> {
                mockConsumer.rebalance(Collections.emptyList());
                partitionsRevoked.countDown();
            });
            assertTrue(awaitLatch(partitionsRevoked));

            subscription.request(1L);
            assertFalse(secondReceived.await(100L, TimeUnit.MILLISECONDS));
            assertNull(failure.get());
        } finally {
            subscription.cancel();
        }
    }

    @Test
    public void rebalance_givenZeroGraceAndUnacknowledgedRecord_expectsTransactionAbortedWithoutCommit() {
        String topic = "topic";
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(new TopicPartition(topic, 0), 0L);

        KafkaTxManager txManager = mock(KafkaTxManager.class);
        when(txManager.begin()).thenReturn(Mono.empty());
        when(txManager.sendOffsets(any(), any())).thenReturn(Mono.empty());
        when(txManager.commit()).thenReturn(Mono.empty());
        when(txManager.abort()).thenReturn(Mono.empty());

        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(beginningOffsets.keySet());
            mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "value"));
        });

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
                .fullPollRecordsPrefetch(1)
                .commitBatchSize(1)
                .revocationGracePeriod(Duration.ZERO)
                .build();

        CountDownLatch recordReceived = new CountDownLatch(1);
        CountDownLatch failureReceived = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Subscriber<KafkaReceiverRecord<String, String>> subscriber = new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {}

            @Override
            public void onNext(KafkaReceiverRecord<String, String> record) {
                recordReceived.countDown();
            }

            @Override
            public void onError(Throwable error) {
                failure.set(error);
                failureReceived.countDown();
            }

            @Override
            public void onComplete() {}
        };
        Subscription subscription = new PollingSubscriptionFactory<>(options)
                .transactional(txManager, ConsumptionSpec.subscribe(Collections.singletonList(topic)), subscriber);

        try {
            subscription.request(1L);
            assertTrue(awaitLatch(recordReceived));

            mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.emptyList()));
            assertTrue(awaitLatch(failureReceived));

            Throwable unwrappedFailure = Exceptions.unwrap(failure.get());
            assertTrue(unwrappedFailure instanceof TimeoutException);
            assertEquals("Revocation deactivation timeout", unwrappedFailure.getMessage());
            verify(txManager, never()).commit();
            verify(txManager).abort();
        } finally {
            subscription.cancel();
        }
    }

    @Test
    public void cancel_givenShorterTerminationGrace_expectsTerminationGraceUsedForTransactionalDeactivation()
            throws InterruptedException {
        String topic = "topic";
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        Map<TopicPartition, Long> beginningOffsets = Collections.singletonMap(topicPartition, 0L);

        KafkaTxManager txManager = mock(KafkaTxManager.class);
        when(txManager.begin()).thenReturn(Mono.empty());
        when(txManager.sendOffsets(any(), any())).thenReturn(Mono.empty());
        when(txManager.commit()).thenReturn(Mono.empty());
        when(txManager.abort()).thenReturn(Mono.empty());

        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(new ConsumerRecord<>(topic, 0, 0L, "key", "value")));

        CountDownLatch consumerClosed = new CountDownLatch(1);
        ConsumerListener consumerListener = new ConsumerListener() {
            @Override
            public void close() {
                consumerClosed.countDown();
            }
        };

        KafkaReceiverOptions<String, String> options = KafkaReceiverOptions.newBuilder(__ -> mockConsumer)
                .consumerListener(consumerListener)
                .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "test")
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1)
                .fullPollRecordsPrefetch(1)
                .commitBatchSize(1)
                .revocationGracePeriod(Duration.ofSeconds(5L))
                .terminationGracePeriod(Duration.ofMillis(100L))
                .build();

        CountDownLatch recordReceived = new CountDownLatch(1);
        AtomicReference<KafkaReceiverRecord<String, String>> received = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Subscriber<KafkaReceiverRecord<String, String>> subscriber = new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {}

            @Override
            public void onNext(KafkaReceiverRecord<String, String> record) {
                received.set(record);
                recordReceived.countDown();
            }

            @Override
            public void onError(Throwable error) {
                failure.set(error);
            }

            @Override
            public void onComplete() {}
        };
        Subscription subscription = new PollingSubscriptionFactory<>(options)
                .transactional(
                        txManager, ConsumptionSpec.assign(Collections.singletonList(topicPartition)), subscriber);

        try {
            subscription.request(1L);
            assertTrue(awaitLatch(recordReceived));

            subscription.cancel();

            assertTrue(consumerClosed.await(2L, TimeUnit.SECONDS));
            assertNull(failure.get());
            verify(txManager).abort();
            verify(txManager, never()).commit();
        } finally {
            KafkaReceiverRecord<String, String> record = received.get();
            if (record != null) {
                record.acknowledge();
            }
            consumerClosed.await(10L, TimeUnit.SECONDS);
        }
    }

    private static void schedulePollEventing(MockConsumer<String, String> mockConsumer, Sinks.Many<Long> polled) {
        mockConsumer.schedulePollTask(() -> {
            polled.tryEmitNext(System.currentTimeMillis());
            schedulePollEventing(mockConsumer, polled);
        });
    }

    private static boolean awaitLatch(CountDownLatch latch) {
        try {
            return latch.await(10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static OffsetTrackingStrategy skippingOffsetTrackingStrategy(long skippedOffset) {
        return (consumer, partitions) -> partitions.stream()
                .map(partition -> new SkippingOffsetTracker(partition, skippedOffset))
                .collect(Collectors.toList());
    }

    private record SkippingOffsetTracker(TopicPartition topicPartition, long skippedOffset) implements OffsetTracker {

        @Override
        public boolean prohibitsProcessing(long offset) {
            return offset == skippedOffset;
        }

        @Override
        public void acknowledged(long offset) {}

        @Override
        public Mono<String> commitMetadata(long commitOffset) {
            return Mono.just("");
        }

        @Override
        public Optional<ConsumerOffset> initialConsumerOffset() {
            return Optional.empty();
        }
    }
}
