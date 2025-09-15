package io.atleon.kafka;

import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaReceiverTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    @Test
    public void receiveAutoAck_givenSuccessfulProcessing_expectsProcessingContinuation() {
        String topic = UUID.randomUUID().toString();

        KafkaSenderRecord<String, String, Object> senderRecord1 =
            KafkaSenderRecord.create(topic, "key", "value1", null);
        KafkaSenderRecord<String, String, Object> senderRecord2 =
            KafkaSenderRecord.create(topic, "key", "value2", null);

        sendStrings(senderRecord1, senderRecord2);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newStringReceiverOptionsBuilder()
            .consumerListener(closureListener)
            .build();

        KafkaReceiver.create(receiverOptions)
            .receiveAutoAck(Collections.singletonList(topic))
            .take(1)
            .concatMap(Function.identity())
            .as(StepVerifier::create)
            .consumeNextWith(it -> assertEquals(senderRecord1.value(), it.value()))
            .expectComplete()
            .verify();

        assertNull(closureListener.closed().block());

        KafkaReceiver.create(receiverOptions)
            .receiveAutoAck(Collections.singletonList(topic))
            .concatMap(Function.identity())
            .as(StepVerifier::create)
            .consumeNextWith(it -> assertEquals(senderRecord2.value(), it.value()))
            .thenCancel()
            .verify();
    }

    @Test
    public void receiveAutoAck_givenFailedProcessing_expectsReprocessing() {
        String topic = UUID.randomUUID().toString();

        KafkaSenderRecord<String, String, Object> senderRecord =
            KafkaSenderRecord.create(topic, "key", "value1", null);

        sendStrings(senderRecord);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newStringReceiverOptionsBuilder()
            .consumerListener(closureListener)
            .build();

        Flux<?> flux = KafkaReceiver.create(receiverOptions)
            .receiveAutoAck(Collections.singletonList(topic))
            .doOnNext(it -> {
                throw new UnsupportedOperationException("Boom");
            });

        assertThrows(UnsupportedOperationException.class, flux::blockLast);
        assertNull(closureListener.closed().block());

        KafkaReceiver.create(receiverOptions)
            .receiveAutoAck(Collections.singletonList(topic))
            .concatMap(Function.identity())
            .as(StepVerifier::create)
            .consumeNextWith(it -> assertEquals(senderRecord.value(), it.value()))
            .thenCancel()
            .verify();
    }

    @Test
    public void receiveAutoAck_givenCommitlessOffsets_expectsStatelessProcessing() {
        String topic = UUID.randomUUID().toString();

        KafkaSenderRecord<String, String, Object> senderRecord1 =
            KafkaSenderRecord.create(topic, "key", "value1", null);

        sendStrings(senderRecord1);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newStringReceiverOptionsBuilder()
            .consumerListener(closureListener)
            .commitlessOffsets(true)
            .build();

        KafkaReceiver.create(receiverOptions)
            .receiveAutoAck(Collections.singletonList(topic))
            .concatMap(Function.identity())
            .as(StepVerifier::create)
            .consumeNextWith(it -> assertEquals(senderRecord1.value(), it.value()))
            .thenCancel()
            .verify();

        assertNull(closureListener.closed().block());

        KafkaReceiver.create(receiverOptions)
            .receiveAutoAck(Collections.singletonList(topic))
            .concatMap(Function.identity())
            .as(StepVerifier::create)
            .consumeNextWith(it -> assertEquals(senderRecord1.value(), it.value()))
            .thenCancel()
            .verify();
    }

    @Test
    public void receiveManual_givenUnacknowledgedRecord_expectsReprocessing() {
        String topic = UUID.randomUUID().toString();

        KafkaSenderRecord<String, String, Object> senderRecord =
            KafkaSenderRecord.create(topic, "key", "value", null);

        sendStrings(senderRecord);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newStringReceiverOptionsBuilder()
            .consumerListener(closureListener)
            .build();

        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .expectNextMatches(it -> it.consumerRecord().key().equals(senderRecord.key()))
            .thenCancel()
            .verify();

        assertNull(closureListener.closed().block());

        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .expectNextMatches(it -> it.consumerRecord().key().equals(senderRecord.key()))
            .thenCancel()
            .verify();
    }

    @Test
    public void receiveManual_givenAcknowledgedRecord_expectsProcessingContinuation() {
        String topic = UUID.randomUUID().toString();

        KafkaSenderRecord<String, String, Object> senderRecord1 =
            KafkaSenderRecord.create(topic, "key", "value1", null);
        KafkaSenderRecord<String, String, Object> senderRecord2 =
            KafkaSenderRecord.create(topic, "key", "value2", null);

        sendStrings(senderRecord1, senderRecord2);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newStringReceiverOptionsBuilder()
            .consumerListener(closureListener)
            .build();

        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(topic))
            .doOnNext(KafkaReceiverRecord::acknowledge)
            .as(StepVerifier::create)
            .expectNextMatches(it -> it.consumerRecord().key().equals(senderRecord1.key()))
            .thenCancel()
            .verify();

        assertNull(closureListener.closed().block());

        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .expectNextMatches(it -> it.consumerRecord().key().equals(senderRecord2.key()))
            .thenCancel()
            .verify();
    }

    @Test
    public void receiveManual_givenNegativelyAcknowledgedRecord_expectsErrorAndReprocessing() {
        String topic = UUID.randomUUID().toString();

        KafkaSenderRecord<String, String, Object> senderRecord1 =
            KafkaSenderRecord.create(topic, "key", "value1", null);
        KafkaSenderRecord<String, String, Object> senderRecord2 =
            KafkaSenderRecord.create(topic, "key", "value2", null);

        sendStrings(senderRecord1, senderRecord2);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newStringReceiverOptionsBuilder()
            .consumerListener(closureListener)
            .build();

        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .consumeNextWith(it -> it.nacknowledge(new UnsupportedOperationException("Boom")))
            .expectError(UnsupportedOperationException.class)
            .verify();

        assertNull(closureListener.closed().block());

        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .expectNextMatches(it -> it.consumerRecord().key().equals(senderRecord2.key()))
            .thenCancel()
            .verify();
    }

    @Test
    public void receiveManual_givenOutOfOrderAcknowledgements_expectsCorrectProcessingContinuation() {
        String topic = UUID.randomUUID().toString();

        KafkaSenderRecord<String, String, Object> senderRecord1 =
            KafkaSenderRecord.create(topic, "key", "value1", null);
        KafkaSenderRecord<String, String, Object> senderRecord2 =
            KafkaSenderRecord.create(topic, "key", "value2", null);
        KafkaSenderRecord<String, String, Object> senderRecord3 =
            KafkaSenderRecord.create(topic, "key", "value3", null);

        sendStrings(senderRecord1, senderRecord2, senderRecord3);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newStringReceiverOptionsBuilder()
            .consumerListener(closureListener)
            .build();

        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .expectNextMatches(it -> it.consumerRecord().key().equals(senderRecord2.key()))
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .thenCancel()
            .verify();

        assertNull(closureListener.closed().block());

        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .expectNextMatches(it -> it.consumerRecord().key().equals(senderRecord2.key()))
            .thenCancel()
            .verify();
    }

    @Test
    public void receiveManual_givenTooManyUnacknowledgedRecords_expectsPausedEmission() {
        String topic = UUID.randomUUID().toString();

        sendStrings(
            KafkaSenderRecord.create(topic, "key", "value1", null),
            KafkaSenderRecord.create(topic, "key", "value2", null));

        KafkaReceiverOptions<String, String> receiverOptions = newStringReceiverOptionsBuilder()
            .maxActiveInFlight(1)
            .build();

        AtomicReference<KafkaReceiverRecord<String, String>> firstRecord = new AtomicReference<>();
        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .consumeNextWith(firstRecord::set)
            .expectNoEvent(receiverOptions.pollTimeout().multipliedBy(2))
            .then(() -> firstRecord.get().acknowledge())
            .expectNextMatches(it -> it.value().equals("value2"))
            .thenCancel()
            .verify();
    }

    @Test
    public void receiveManual_givenCommitTriggering_expectsCommittedOffsets() {
        String topic = UUID.randomUUID().toString();
        String groupId = UUID.randomUUID().toString();

        sendStrings(
            KafkaSenderRecord.create(topic, "key", "value1", null),
            KafkaSenderRecord.create(topic, "key", "value2", null));

        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newStringConsumerProperties(groupId))
            .commitBatchSize(2)
            .build();

        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(topic))
            .as(StepVerifier::create)
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .thenAwait(Duration.ofSeconds(1L))
            .then(() -> {
                try (ReactiveAdmin admin = ReactiveAdmin.create(newKafkaProperties())) {
                    Long maxOffset = admin.listTopicPartitionGroupOffsets(groupId)
                        .reduce(0L, (max, next) -> Math.max(max, next.groupOffset()))
                        .block();
                    assertEquals(2, maxOffset);
                }
            })
            .thenCancel()
            .verify();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void receiveTxManual_givenCommitmentBatchTriggering_expectsTransactionExecution(boolean commitlessOffsets) {
        String topic = UUID.randomUUID().toString();
        String groupId = UUID.randomUUID().toString();
        int partition = 0;

        sendStrings(
            KafkaSenderRecord.create(topic, partition, "key", "value1", null),
            KafkaSenderRecord.create(topic, partition, "key", "value2", null));

        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newStringConsumerProperties(groupId))
            .commitBatchSize(2)
            .commitlessOffsets(commitlessOffsets)
            .build();

        KafkaTxManager txManager = mock(KafkaTxManager.class);
        when(txManager.begin()).thenReturn(Mono.empty());
        when(txManager.sendOffsets(any(), any())).thenReturn(Mono.empty());
        when(txManager.commit()).thenReturn(Mono.empty());

        KafkaReceiver.create(receiverOptions)
            .receiveTxManual(Mono.just(txManager), Collections.singletonList(topic))
            .as(StepVerifier::create)
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .thenAwait(Duration.ofSeconds(1L))
            .thenCancel()
            .verify();

        verify(txManager, times(1)).begin();
        verify(txManager, commitlessOffsets ? never() : times(1)).sendOffsets(
            argThat(it -> it.get(new TopicPartition(topic, partition)).offset() == 2),
            argThat(it -> it.groupId().equals(groupId))
        );
        verify(txManager, times(1)).commit();
        verify(txManager, never()).abort();
    }

    @Test
    public void receiveTxManual_givenCommitmentPeriodTriggering_expectsTransactionExecution() {
        String topic = UUID.randomUUID().toString();
        String groupId = UUID.randomUUID().toString();
        int partition = 0;

        sendStrings(
            KafkaSenderRecord.create(topic, partition, "key", "value1", null),
            KafkaSenderRecord.create(topic, partition, "key", "value2", null));

        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newStringConsumerProperties(groupId))
            .commitPeriod(Duration.ofMillis(100))
            .build();

        KafkaTxManager txManager = mock(KafkaTxManager.class);
        when(txManager.begin()).thenReturn(Mono.empty());
        when(txManager.sendOffsets(any(), any())).thenReturn(Mono.empty());
        when(txManager.commit()).thenReturn(Mono.empty());

        KafkaReceiver.create(receiverOptions)
            .receiveTxManual(Mono.just(txManager), Collections.singletonList(topic))
            .as(it -> StepVerifier.create(it, 0))
            .thenRequest(1)
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .thenAwait(receiverOptions.commitPeriod().multipliedBy(2))
            .thenRequest(1)
            .consumeNextWith(KafkaReceiverRecord::acknowledge)
            .thenAwait(receiverOptions.commitPeriod().multipliedBy(5))
            .thenCancel()
            .verify();

        verify(txManager, times(2)).begin();
        verify(txManager, times(1)).sendOffsets(
            argThat(it -> it.get(new TopicPartition(topic, partition)).offset() == 1),
            argThat(it -> it.groupId().equals(groupId))
        );
        verify(txManager, times(1)).sendOffsets(
            argThat(it -> it.get(new TopicPartition(topic, partition)).offset() == 2),
            argThat(it -> it.groupId().equals(groupId))
        );
        verify(txManager, times(2)).commit();
        verify(txManager, never()).abort();
    }

    @Test
    public void receiveTxManual_givenAbortionTriggering_expectsTransactionExecution() {
        String topic = UUID.randomUUID().toString();
        String groupId = UUID.randomUUID().toString();
        int partition = 0;

        sendStrings(KafkaSenderRecord.create(topic, partition, "key", "value", null));

        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newStringConsumerProperties(groupId))
            .commitBatchSize(2)
            .build();

        KafkaTxManager txManager = mock(KafkaTxManager.class);
        when(txManager.begin()).thenReturn(Mono.empty());
        when(txManager.abort()).thenReturn(Mono.empty());

        KafkaReceiver.create(receiverOptions)
            .receiveTxManual(Mono.just(txManager), Collections.singletonList(topic))
            .as(StepVerifier::create)
            .consumeNextWith(it -> it.nacknowledge(new UnsupportedOperationException("Boom")))
            .expectError(UnsupportedOperationException.class)
            .verify();

        verify(txManager, times(1)).begin();
        verify(txManager, timeout(1000).times(1)).abort();
    }

    @Test
    public void receiveManualInRange_givenEarliestToLatestRange_expectsConsumptionOfAllData() {
        String topic = UUID.randomUUID().toString();
        Set<Long> producedValues = sendLongs(
            createRandomLongValues(1000, Collectors.toSet()),
            value -> KafkaSenderRecord.create(topic, value, value, value)
        );

        OffsetRange offsetRange = OffsetCriteria.earliest().to(OffsetCriteria.latest());
        Set<Long> result = receiveLongValues(topic, offsetRange, Collectors.toSet());

        assertEquals(producedValues, result);
    }

    @Test
    public void receiveManualInRange_givenSingletonEarliestCriteria_expectsConsumptionOfEarliestRecord() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = sendLongs(
            createRandomLongValues(4, Collectors.toList()),
            value -> KafkaSenderRecord.create(topic, 0, value, value, value)
        );

        List<Long> result = receiveLongValues(topic, OffsetCriteria.earliest().asRange());

        assertEquals(producedValues.subList(0, 1), result);
    }

    @Test
    public void receiveManualInRange_givenSingletonLatestCriteria_expectsConsumptionOfLatestRecord() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = sendLongs(
            createRandomLongValues(4, Collectors.toList()),
            value -> KafkaSenderRecord.create(topic, 0, value, value, value)
        );

        List<Long> result = receiveLongValues(topic, OffsetCriteria.latest().asRange());

        assertEquals(producedValues.subList(producedValues.size() - 1, producedValues.size()), result);
    }

    @Test
    public void receiveManualInRange_givenSingletonRawOffsetCriteria_expectsConsumptionOfRecordAtOffset() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = sendLongs(
            createRandomLongValues(4, Collectors.toList()),
            value -> KafkaSenderRecord.create(topic, 0, value, value, value)
        );

        List<Long> result = receiveLongValues(topic, OffsetCriteria.raw(1).asRange());

        assertEquals(producedValues.subList(1, 2), result);
    }

    @Test
    public void receiveManualInRange_givenRawOffsetRanges_expectsConsumptionOfRecordsInOffsetRange() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = sendLongs(
            createRandomLongValues(4, Collectors.toList()),
            value -> KafkaSenderRecord.create(topic, 0, value, value, value)
        );

        assertEquals(
            producedValues.subList(0, 2),
            receiveLongValues(topic, OffsetCriteria.raw(0).to(OffsetCriteria.raw(1)))
        );
        assertEquals(
            producedValues.subList(1, 3),
            receiveLongValues(topic, OffsetCriteria.raw(1).to(OffsetCriteria.raw(2)))
        );
        assertEquals(
            producedValues.subList(2, 4),
            receiveLongValues(topic, OffsetCriteria.raw(2).to(OffsetCriteria.raw(3)))
        );
        assertEquals(
            producedValues.subList(3, 4),
            receiveLongValues(topic, OffsetCriteria.raw(3).to(OffsetCriteria.raw(4)))
        );
    }

    @Test
    public void receiveManualInRange_givenSingletonTimestampCriteria_expectsConsumptionOfRecordAtTimestamp() {
        String topic = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        List<Long> producedTimestamps = sendLongs(
            IntStream.range(0, 3).mapToObj(index -> now + (index * 2L)).collect(Collectors.toList()),
            epoch -> KafkaSenderRecord.create(new ProducerRecord<>(topic, 0, epoch, epoch, epoch), null)
        );

        List<Long> result = receiveLongValues(topic, OffsetCriteria.timestamp(producedTimestamps.get(1)).asRange());

        assertEquals(producedTimestamps.subList(1, 2), result);
    }

    @Test
    public void receiveManualInRange_givenTimestampRanges_expectsConsumptionOfRecordsInTimestampRange() {
        String topic = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        List<Long> producedTimestamps = sendLongs(
            IntStream.range(0, 3).mapToObj(index -> now + (index * 2L)).collect(Collectors.toList()),
            epoch -> KafkaSenderRecord.create(new ProducerRecord<>(topic, 0, epoch, epoch, epoch), null)
        );

        assertEquals(
            Collections.emptyList(),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(0) - 2, producedTimestamps.get(0) - 1))
        );
        assertEquals(
            producedTimestamps.subList(0, 1),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(0) - 1, producedTimestamps.get(0)))
        );
        assertEquals(
            producedTimestamps.subList(0, 1),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(0) - 1, producedTimestamps.get(0) + 1))
        );
        assertEquals(
            producedTimestamps.subList(0, 1),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(0), producedTimestamps.get(0) + 1))
        );
        assertEquals(
            producedTimestamps.subList(0, 2),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(0), producedTimestamps.get(1)))
        );
        assertEquals(
            producedTimestamps.subList(1, 2),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(1) - 1, producedTimestamps.get(1) + 1))
        );
        assertEquals(
            producedTimestamps.subList(1, 3),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(1), producedTimestamps.get(2)))
        );
        assertEquals(
            producedTimestamps.subList(2, 3),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(2) - 1, producedTimestamps.get(2)))
        );
        assertEquals(
            producedTimestamps.subList(2, 3),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(2) - 1, producedTimestamps.get(2) + 1))
        );
        assertEquals(
            producedTimestamps.subList(2, 3),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(2), producedTimestamps.get(2) + 1))
        );
        assertEquals(
            Collections.emptyList(),
            receiveLongValues(topic, timestampRange(producedTimestamps.get(2) + 1, producedTimestamps.get(2) + 2))
        );
        assertEquals(
            producedTimestamps,
            receiveLongValues(topic, timestampRange(producedTimestamps.get(0), producedTimestamps.get(2)))
        );
    }

    @Test
    public void receiveManualInRange_givenCriteriaBasedOnConsumerGroup_expects() {
        TopicPartition topicPartition = new TopicPartition(UUID.randomUUID().toString(), 0);
        String groupId = UUID.randomUUID().toString();
        List<Long> producedValues = sendLongs(
            createRandomLongValues(4, Collectors.toList()),
            value -> KafkaSenderRecord.create(topicPartition.topic(), topicPartition.partition(), value, value, value)
        );

        Mono<Void> alterGroupOffsets = Mono.using(
            () -> ReactiveAdmin.create(newKafkaProperties()),
            it -> it.alterRawConsumerGroupOffsets(groupId, Collections.singletonMap(topicPartition, 2L)),
            ReactiveAdmin::close);
        alterGroupOffsets.block();

        OffsetCriteria consumerGroupCriteria = OffsetCriteria.consumerGroup(groupId, OffsetResetStrategy.EARLIEST);

        assertEquals(
            producedValues.subList(0, 2),
            receiveLongValues(topicPartition.topic(), OffsetCriteria.earliest().to(consumerGroupCriteria))
        );
        assertEquals(
            producedValues.subList(2, 4),
            receiveLongValues(topicPartition.topic(), consumerGroupCriteria.to(OffsetCriteria.latest()))
        );
    }

    @Test
    public void receiveManualInRange_givenConsecutiveConsumerGroupEarliestToLatestRanges_expectsResumption() {
        String topic = UUID.randomUUID().toString();
        String groupId = UUID.randomUUID().toString();
        Set<Long> firstValues = sendLongs(
            createRandomLongValues(10, Collectors.toSet()),
            value -> KafkaSenderRecord.create(topic, value, value, value)
        );

        KafkaReceiverOptions<Long, Long> receiverOptions = newLongReceiverOptionsBuilder()
            .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            .build();
        OffsetRange offsetRange = OffsetCriteria.consumerGroup(groupId, OffsetResetStrategy.EARLIEST)
            .to(OffsetCriteria.latest());
        OffsetRangeProvider offsetRangeProvider = OffsetRangeProvider.inOffsetRangeFromAllTopicPartitions(offsetRange);

        Set<Long> firstResult = receiveLongValues(receiverOptions, topic, offsetRangeProvider, Collectors.toSet());

        Set<Long> secondValues = sendLongs(
            createRandomLongValues(5, Collectors.toSet()),
            value -> KafkaSenderRecord.create(topic, value, value, value)
        );

        Set<Long> secondResult = receiveLongValues(receiverOptions, topic, offsetRangeProvider, Collectors.toSet());

        assertEquals(firstValues, firstResult);
        assertEquals(secondValues, secondResult);
    }

    @Test
    public void receiveManualInRange_givenRangesOverASinglePartition_expectsConsumptionOfRecordsInRangeOnPartition() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = sendLongs(
            LongStream.range(0, 6).boxed().collect(Collectors.toList()),
            value -> KafkaSenderRecord.create(topic, (int) (value / 3), value, value, value)
        );

        OffsetRange earliestToLatest = OffsetCriteria.earliest().to(OffsetCriteria.latest());
        OffsetRangeProvider allValuesFromFirstPartition = OffsetRangeProvider.newBuilder()
            .inOffsetRangeFromTopicPartition(new TopicPartition(topic, 0), earliestToLatest)
            .build();
        OffsetRangeProvider partialValuesFromSecondPartition = OffsetRangeProvider.newBuilder()
            .inOffsetRangeFromTopicPartition(new TopicPartition(topic, 1), 1, OffsetCriteria.latest())
            .build();

        assertEquals(producedValues.subList(0, 3), receiveLongValues(topic, allValuesFromFirstPartition));
        assertEquals(producedValues.subList(4, 6), receiveLongValues(topic, partialValuesFromSecondPartition));
    }

    @Test
    public void receiveManualInRange_givenStartingOffsetRangeProvider_expectsResumptionOfConsumption() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = sendLongs(
            LongStream.range(0, 6).boxed().collect(Collectors.toList()),
            value -> KafkaSenderRecord.create(topic, (int) (value / 3), value, value, value)
        );

        OffsetRangeProvider startingInFirstPartition = OffsetRangeProvider.startingFromRawOffsetInTopicPartition(
            new TopicPartition(topic, 0),
            2,
            OffsetCriteria.earliest().to(OffsetCriteria.latest())
        );

        assertEquals(producedValues.subList(2, 6), receiveLongValues(topic, startingInFirstPartition));
    }

    @Test
    public void receiveManualInRange_givenRawOffsetsInSinglePartition_expectsConsumptionOfRecordsInRawOffsetRange() {
        String topic = UUID.randomUUID().toString();
        List<Long> producedValues = sendLongs(
            LongStream.range(0, 6).boxed().collect(Collectors.toList()),
            value -> KafkaSenderRecord.create(topic, (int) (value / 3), value, value, value)
        );

        OffsetRangeProvider recordsInFirstPartition = OffsetRangeProvider.usingRawOffsetRange(
            new TopicPartition(topic, 0),
            RawOffsetRange.of(1, 2)
        );

        assertEquals(producedValues.subList(1, 3), receiveLongValues(topic, recordsInFirstPartition));
    }

    @SafeVarargs
    private static <T> void sendStrings(KafkaSenderRecord<String, String, T>... senderRecords) {
        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
            .producerProperties(newKafkaProperties())
            .producerProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .producerProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
            .build();

        sendRecords(senderOptions, Flux.just(senderRecords));
    }

    private static <C extends Collection<Long>> C sendLongs(
        C values,
        Function<Long, KafkaSenderRecord<Long, Long, Long>> recordCreator
    ) {
        KafkaSenderOptions<Long, Long> senderOptions = KafkaSenderOptions.<Long, Long>newBuilder()
            .producerProperties(newKafkaProperties())
            .producerProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName())
            .producerProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName())
            .build();

        sendRecords(senderOptions, Flux.fromIterable(values).map(recordCreator));

        return values;
    }

    private static <K, V, T> List<KafkaSenderResult<T>> sendRecords(
        KafkaSenderOptions<K, V> senderOptions,
        Flux<KafkaSenderRecord<K, V, T>> senderRecords
    ) {
        KafkaSender<K, V> sender = KafkaSender.create(senderOptions);
        try {
            return senderRecords.as(sender::send).collectList().block();
        } finally {
            sender.close();
        }
    }

    private static List<Long> receiveLongValues(String topic, OffsetRange offsetRange) {
        return receiveLongValues(topic, offsetRange, Collectors.toList());
    }

    private static <C extends Collection<Long>> C receiveLongValues(
        String topic,
        OffsetRange offsetRange,
        Collector<Long, ?, C> collector
    ) {
        OffsetRangeProvider rangeProvider = OffsetRangeProvider.newBuilder()
            .inOffsetRangeFromAllTopicPartitions(offsetRange)
            .maxConcurrentTopicPartitions(Integer.MAX_VALUE)
            .build();
        return receiveLongValues(topic, rangeProvider, collector);
    }

    private static List<Long> receiveLongValues(String topic, OffsetRangeProvider offsetRangeProvider) {
        return receiveLongValues(topic, offsetRangeProvider, Collectors.toList());
    }

    private static <C extends Collection<Long>> C receiveLongValues(
        String topic,
        OffsetRangeProvider offsetRangeProvider,
        Collector<Long, ?, C> collector
    ) {
        KafkaReceiverOptions<Long, Long> receiverOptions = newLongReceiverOptionsBuilder()
            .commitlessOffsets(true)
            .build();
        return receiveLongValues(receiverOptions, topic, offsetRangeProvider, collector);
    }

    private static <C extends Collection<Long>> C receiveLongValues(
        KafkaReceiverOptions<Long, Long> receiverOptions,
        String topic,
        OffsetRangeProvider offsetRangeProvider,
        Collector<Long, ?, C> collector
    ) {
        return KafkaReceiver.create(receiverOptions)
            .receiveManualInRanges(Collections.singletonList(topic), offsetRangeProvider)
            .doOnNext(KafkaReceiverRecord::acknowledge)
            .map(KafkaReceiverRecord::value)
            .collect(collector)
            .block();
    }

    private static KafkaReceiverOptions.Builder<String, String> newStringReceiverOptionsBuilder() {
        return KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newStringConsumerProperties(UUID.randomUUID().toString()));
    }

    private static Map<String, Object> newStringConsumerProperties(String groupId) {
        Map<String, Object> consumerProperties = newConsumerProperties(groupId);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return consumerProperties;
    }

    private static KafkaReceiverOptions.Builder<Long, Long> newLongReceiverOptionsBuilder() {
        return KafkaReceiverOptions.<Long, Long>newBuilder()
            .consumerProperties(newKafkaProperties())
            .consumerProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName())
            .consumerProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    }

    private static Map<String, Object> newConsumerProperties(String groupId) {
        Map<String, Object> consumerProperties = newKafkaProperties();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerProperties;
    }

    private static Map<String, Object> newKafkaProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_CONNECT);
        return properties;
    }

    private static <T> T createRandomLongValues(int count, Collector<Long, ?, T> collector) {
        Random random = new Random();
        return IntStream.range(0, count).mapToObj(unused -> random.nextLong()).collect(collector);
    }

    private static OffsetRange timestampRange(long minEpochMillis, long maxEpochMillis) {
        return OffsetCriteria.timestamp(minEpochMillis).to(OffsetCriteria.timestamp(maxEpochMillis));
    }
}