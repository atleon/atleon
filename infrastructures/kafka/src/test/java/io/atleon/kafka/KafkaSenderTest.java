package io.atleon.kafka;

import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaSenderTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    @Test
    public void send_givenDownstreamContext_expectsPropagationToUpstream() {
        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
                .producerProperties(newProducerProperties())
                .build();
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        Flux<KafkaSenderRecord<String, String, Object>> senderRecords = Flux.deferContextual(context -> {
            assertEquals("context", context.get(String.class));
            return Flux.empty();
        });

        Mono<Void> result = sender.send(senderRecords)
                .contextWrite(Context.of(String.class, "context"))
                .then();

        assertDoesNotThrow(() -> result.block());

        sender.close();
    }

    @Test
    public void send_givenSuccessfulSend_expectsProducedRecord() {
        String topic = UUID.randomUUID().toString();
        int partition = 0;

        KafkaSenderRecord<String, String, Object> senderRecord =
                KafkaSenderRecord.create(topic, partition, "key", "value", null);

        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
                .producerProperties(newProducerProperties())
                .build();
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        sender.send(senderRecord).block();

        assertEquals(1, countRecords(topic, partition));

        sender.close();
    }

    @Test
    public void send_givenFailedSend_expectsSubsequentRecordsToNotBeProduced() {
        String topic = UUID.randomUUID().toString();
        int partition = 0;

        KafkaSenderRecord<String, String, Object> senderRecord1 =
                KafkaSenderRecord.create(topic, partition, "key1", "value1", null);
        KafkaSenderRecord<String, String, Object> senderRecord2 =
                KafkaSenderRecord.create(topic, partition, "key2", "Boom", null);
        KafkaSenderRecord<String, String, Object> senderRecord3 =
                KafkaSenderRecord.create(topic, partition, "key3", "value3", null);

        ProducerListener.Closure producerListener = ProducerListener.closure();
        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
                .producerListener(producerListener)
                .producerProperties(newProducerProperties())
                .build();
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        assertThrows(SerializationException.class, () -> Flux.just(senderRecord1, senderRecord2, senderRecord3)
                .as(sender::send)
                .then()
                .block());

        sender.close();
        producerListener.closed().then(Mono.delay(Duration.ofSeconds(1))).block();

        assertEquals(1, countRecords(topic, partition));
    }

    @Test
    public void send_givenManyProducedRecords_expectsCorrectUpstreamRequestCount() throws Exception {
        String topic = UUID.randomUUID().toString();
        int maxInFlight = 16;
        int numRecords = 100;

        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
                .producerProperties(newProducerProperties())
                .maxInFlight(maxInFlight)
                .build();
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        Sinks.Many<KafkaSenderRecord<String, String, Object>> sink =
                Sinks.many().unicast().onBackpressureBuffer();

        AtomicLong requested = new AtomicLong(0L);
        CountDownLatch latch = new CountDownLatch(numRecords);
        sink.asFlux().doOnRequest(requested::addAndGet).as(sender::send).subscribe(__ -> latch.countDown());

        Flux.range(0, numRecords)
                .map(i -> KafkaSenderRecord.create(topic, "key" + i, "value" + i, null))
                .subscribe(sink::tryEmitNext);

        latch.await();
        sender.close();

        assertEquals(senderOptions.maxInFlight() + numRecords, requested.get(), 1);
    }

    @Test
    public void sendDelayError_givenFailedSend_expectsSubsequentRecordsToBeProduced() {
        String topic = UUID.randomUUID().toString();
        int partition = 0;

        KafkaSenderRecord<String, String, Object> senderRecord1 =
                KafkaSenderRecord.create(topic, partition, "key1", "value1", null);
        KafkaSenderRecord<String, String, Object> senderRecord2 =
                KafkaSenderRecord.create(topic, partition, "key2", "Boom", null);
        KafkaSenderRecord<String, String, Object> senderRecord3 =
                KafkaSenderRecord.create(topic, partition, "key3", "value3", null);

        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
                .producerProperties(newProducerProperties())
                .build();
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        assertThrows(SerializationException.class, () -> Flux.just(senderRecord1, senderRecord2, senderRecord3)
                .as(sender::sendDelayError)
                .then()
                .block());

        assertEquals(2, countRecords(topic, partition));
    }

    @Test
    public void sendDelegateError_givenFailedSend_expectsSendResultsToReflectErrors() {
        String topic = UUID.randomUUID().toString();
        int partition = 0;

        KafkaSenderRecord<String, String, Boolean> senderRecord1 =
                KafkaSenderRecord.create(topic, partition, "key1", "value1", false);
        KafkaSenderRecord<String, String, Boolean> senderRecord2 =
                KafkaSenderRecord.create(topic, partition, "key2", "Boom", true);
        KafkaSenderRecord<String, String, Boolean> senderRecord3 =
                KafkaSenderRecord.create(topic, partition, "key3", "value3", false);

        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
                .producerProperties(newProducerProperties())
                .build();
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        Map<Boolean, List<KafkaSenderResult<Boolean>>> results = Flux.just(senderRecord1, senderRecord2, senderRecord3)
                .as(sender::sendDelegateError)
                .collect(Collectors.partitioningBy(KafkaSenderResult::correlationMetadata))
                .block();

        assertEquals(2, results.get(false).size());
        assertTrue(results.get(false).stream().noneMatch(KafkaSenderResult::isFailure));
        assertEquals(1, results.get(true).size());
        assertTrue(results.get(true).get(0).isFailure());
        assertInstanceOf(
                SerializationException.class,
                results.get(true).get(0).failureCause().orElse(null));
        assertEquals(2, countRecords(topic, partition));
    }

    @Test
    public void sendTransactional_givenSuccessfulSend_expectsProducedRecord() {
        String topic = UUID.randomUUID().toString();
        int partition = 0;

        Flux<KafkaSenderRecord<String, String, Object>> senderRecords =
                Flux.just(KafkaSenderRecord.create(topic, partition, "key", "value", null));

        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
                .producerProperties(newProducerProperties())
                .producerProperty(
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                        UUID.randomUUID().toString())
                .build();
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        Flux.just(senderRecords)
                .as(sender::sendTransactional)
                .then(Mono.delay(Duration.ofSeconds(1)))
                .block();

        // There should be two records: One "uncommitted" record, followed by COMMIT record marker
        assertEquals(2, countRecords(topic, partition));

        sender.close();
    }

    @Test
    public void sendTransactional_givenFailedSend_expectsAbortedProduction() {
        String topic = UUID.randomUUID().toString();
        int partition = 0;

        Flux<KafkaSenderRecord<String, String, Object>> senderRecords = Flux.just(
                KafkaSenderRecord.create(topic, partition, "key1", "value1", null),
                KafkaSenderRecord.create(topic, partition, "key2", "Boom", null),
                KafkaSenderRecord.create(topic, partition, "key3", "value3", null));

        ProducerListener.Closure producerListener = ProducerListener.closure();
        KafkaSenderOptions<String, String> senderOptions = KafkaSenderOptions.<String, String>newBuilder()
                .producerListener(producerListener)
                .producerProperties(newProducerProperties())
                .producerProperty(
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                        UUID.randomUUID().toString())
                .build();
        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        Flux.just(senderRecords)
                .as(sender::sendTransactional)
                .as(it -> StepVerifier.create(it, 1))
                .expectNextCount(1)
                .thenRequest(Long.MAX_VALUE)
                .expectError(SerializationException.class)
                .verify();

        sender.close();
        producerListener.closed().then(Mono.delay(Duration.ofSeconds(1))).block();

        // There will be one "uncommitted" record, then an ABORT marker, because the next send
        // fails
        assertEquals(2, countRecords(topic, partition));
    }

    private static long countRecords(String topic, int partition) {
        try (ReactiveAdmin admin = ReactiveAdmin.create(newKafkaProperties())) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            return admin.listOffsets(Collections.singletonList(topicPartition), OffsetSpec.latest())
                    .flatMapIterable(Map::values)
                    .reduce(0L, Long::sum)
                    .blockOptional()
                    .orElse(0L);
        }
    }

    private static Map<String, Object> newProducerProperties() {
        Map<String, Object> producerProperties = newKafkaProperties();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BoomingStringSerializer.class.getName());
        return producerProperties;
    }

    private static Map<String, Object> newKafkaProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_CONNECT);
        return properties;
    }

    public static final class BoomingStringSerializer extends StringSerializer {

        @Override
        public byte[] serialize(String topic, String data) {
            if ("Boom".equalsIgnoreCase(data)) {
                throw new SerializationException("Refusing to serialize 'Boom'");
            }
            return super.serialize(topic, data);
        }
    }
}
