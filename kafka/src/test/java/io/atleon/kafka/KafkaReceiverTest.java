package io.atleon.kafka;

import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

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

        send(senderRecord1, senderRecord2);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newReceiverOptionsBuilder()
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

        send(senderRecord);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newReceiverOptionsBuilder()
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

        send(senderRecord1);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newReceiverOptionsBuilder()
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

        send(senderRecord);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newReceiverOptionsBuilder()
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

        send(senderRecord1, senderRecord2);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newReceiverOptionsBuilder()
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

        send(senderRecord1, senderRecord2);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newReceiverOptionsBuilder()
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

        send(senderRecord1, senderRecord2, senderRecord3);

        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = newReceiverOptionsBuilder()
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

        send(
            KafkaSenderRecord.create(topic, "key", "value1", null),
            KafkaSenderRecord.create(topic, "key", "value2", null));

        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newConsumerProperties(UUID.randomUUID().toString()))
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

        send(
            KafkaSenderRecord.create(topic, "key", "value1", null),
            KafkaSenderRecord.create(topic, "key", "value2", null));

        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newConsumerProperties(groupId))
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

        send(
            KafkaSenderRecord.create(topic, partition, "key", "value1", null),
            KafkaSenderRecord.create(topic, partition, "key", "value2", null));

        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newConsumerProperties(groupId))
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

        send(
            KafkaSenderRecord.create(topic, partition, "key", "value1", null),
            KafkaSenderRecord.create(topic, partition, "key", "value2", null));

        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newConsumerProperties(groupId))
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

        send(KafkaSenderRecord.create(topic, partition, "key", "value", null));

        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newConsumerProperties(groupId))
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

    @SafeVarargs
    private static <T> List<KafkaSenderResult<T>> send(KafkaSenderRecord<String, String, T>... senderRecords) {
        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
            .producerProperties(newProducerProperties())
            .build();

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);
        try {
            return Flux.just(senderRecords).as(sender::send).collectList().block();
        } finally {
            sender.close();
        }
    }

    private static KafkaReceiverOptions.Builder<String, String> newReceiverOptionsBuilder() {
        return KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newConsumerProperties(UUID.randomUUID().toString()));
    }

    private static Map<String, Object> newProducerProperties() {
        Map<String, Object> producerProperties = newKafkaProperties();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return producerProperties;
    }

    private static Map<String, Object> newConsumerProperties(String groupId) {
        Map<String, Object> consumerProperties = newKafkaProperties();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return consumerProperties;
    }

    private static Map<String, Object> newKafkaProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_CONNECT);
        return properties;
    }
}