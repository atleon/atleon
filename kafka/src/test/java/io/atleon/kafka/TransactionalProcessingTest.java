package io.atleon.kafka;

import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TransactionalProcessingTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    @Test
    public void process_givenSuccessfulProcessing_expectsSubsequentContinuation() {
        // Create topics and test data
        String inputTopic = UUID.randomUUID().toString();
        String outputTopic = UUID.randomUUID().toString();

        KafkaSenderRecord<String, String, Object> senderRecord1 =
            KafkaSenderRecord.create(inputTopic, "key", "value1", null);
        KafkaSenderRecord<String, String, Object> senderRecord2 =
            KafkaSenderRecord.create(inputTopic, "key", "value2", null);

        // Create transactional sender and produce test data
        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
            .producerProperties(newProducerProperties(UUID.randomUUID().toString()))
            .build();
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        Flux.just(senderRecord1, senderRecord2).as(it -> sender.sendTransactional(Flux.just(it))).then().block();

        // Create reception options
        String groupId = UUID.randomUUID().toString();
        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newConsumerProperties(groupId))
            .consumerListener(closureListener)
            .commitBatchSize(1)
            .build();

        // Successfully process data to output topic
        KafkaReceiver.create(receiverOptions)
            .receiveTxManual(sender.txManager(), Collections.singletonList(inputTopic))
            .map(it -> KafkaSenderRecord.create(outputTopic, it.key(), it.value(), it))
            .transform(sender::send)
            .as(StepVerifier::create)
            .consumeNextWith(it -> it.correlationMetadata().acknowledge())
            .thenAwait(Duration.ofSeconds(1))
            .thenCancel()
            .verify();

        assertNull(closureListener.closed().block());

        // Verify committed offset
        try (ReactiveAdmin admin = ReactiveAdmin.create(newKafkaProperties())) {
            Long maxOffset = admin.listTopicPartitionGroupOffsets(groupId)
                .reduce(0L, (max, next) -> Math.max(max, next.groupOffset()))
                .block();
            assertEquals(1, maxOffset);
        }

        // Verify processing continuation
        KafkaReceiver.create(receiverOptions)
            .receiveManual(Collections.singletonList(outputTopic))
            .as(StepVerifier::create)
            .expectNextMatches(it -> it.consumerRecord().key().equals(senderRecord1.key()))
            .thenCancel()
            .verify();

        sender.close();
    }

    @Test
    public void process_givenFailedProcessing_expectsReprocessing() {
        // Create topics and test data
        String inputTopic = UUID.randomUUID().toString();
        String outputTopic = UUID.randomUUID().toString();

        KafkaSenderRecord<String, String, Object> senderRecord1 =
            KafkaSenderRecord.create(inputTopic, "key", "value1", null);
        KafkaSenderRecord<String, String, Object> senderRecord2 =
            KafkaSenderRecord.create(inputTopic, "key", "value2", null);

        // Create transactional sender and produce test data
        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
            .producerProperties(newProducerProperties(UUID.randomUUID().toString()))
            .build();
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        Flux.just(senderRecord1, senderRecord2).as(it -> sender.sendTransactional(Flux.just(it))).then().block();

        // Create reception options
        String groupId = UUID.randomUUID().toString();
        ConsumerListener.Closure closureListener = ConsumerListener.closure();
        KafkaReceiverOptions<String, String> receiverOptions = KafkaReceiverOptions.<String, String>newBuilder()
            .consumerProperties(newConsumerProperties(groupId))
            .consumerListener(closureListener)
            .commitBatchSize(2)
            .build();

        // Fail transactional processing of data
        KafkaReceiver.create(receiverOptions)
            .receiveTxManual(sender.txManager(), Collections.singletonList(inputTopic))
            .map(it -> KafkaSenderRecord.create(outputTopic, it.key(), it.value(), it))
            .transform(sender::send)
            .as(it -> StepVerifier.create(it, 1))
            .consumeNextWith(it -> it.correlationMetadata().nacknowledge(new UnsupportedOperationException("Boom")))
            .expectError(UnsupportedOperationException.class)
            .verify();

        assertNull(closureListener.closed().block());

        // Verify expected input offsets and output records
        try (ReactiveAdmin admin = ReactiveAdmin.create(newKafkaProperties())) {
            Long maxOffset = admin.listTopicPartitionGroupOffsets(groupId)
                .reduce(0L, (max, next) -> Math.max(max, next.groupOffset()))
                .block();
            assertEquals(0, maxOffset);

            Map<TopicPartition, Long> outputOffsets = admin.listTopicPartitions(outputTopic)
                .collectList()
                .flatMap(it -> admin.listOffsets(it, OffsetSpec.latest()))
                .block();
            // Uncommitted record written, plus ABORT marker
            assertEquals(2, outputOffsets.values().stream().reduce(0L, Math::max));
        }

        // Verify reprocessing of previously failed record(s)
        KafkaReceiver.create(receiverOptions)
            .receiveTxManual(sender.txManager(), Collections.singletonList(inputTopic))
            .map(it -> KafkaSenderRecord.create(outputTopic, it.key(), it.value(), it))
            .transform(sender::send)
            .as(StepVerifier::create)
            .consumeNextWith(it -> it.correlationMetadata().acknowledge())
            .consumeNextWith(it -> it.correlationMetadata().acknowledge())
            .thenAwait(Duration.ofSeconds(1))
            .thenCancel()
            .verify();

        // Verify expected input offsets and output records
        try (ReactiveAdmin admin = ReactiveAdmin.create(newKafkaProperties())) {
            Long maxGroupOffset = admin.listTopicPartitionGroupOffsets(groupId)
                .reduce(0L, (max, next) -> Math.max(max, next.groupOffset()))
                .block();
            assertEquals(2, maxGroupOffset);

            Map<TopicPartition, Long> outputOffsets = admin.listTopicPartitions(outputTopic)
                .collectList()
                .flatMap(it -> admin.listOffsets(it, OffsetSpec.latest()))
                .block();
            assertEquals(5, outputOffsets.values().stream().reduce(0L, Math::max));
        }

        sender.close();
    }

    private static Map<String, Object> newProducerProperties(String transactionalId) {
        Map<String, Object> producerProperties = newKafkaProperties();
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return producerProperties;
    }

    private static Map<String, Object> newConsumerProperties(String groupId) {
        Map<String, Object> consumerProperties = newKafkaProperties();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
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
