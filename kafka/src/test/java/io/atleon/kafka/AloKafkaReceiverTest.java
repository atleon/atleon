package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;

class AloKafkaReceiverTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    private final String topic = AloKafkaReceiverTest.class.getSimpleName() + UUID.randomUUID();

    @Test
    public void receiveAloRecords_givenMultipleSubscriptionAttempts_expectsEnforcementOfMutualExclusion() {
        KafkaConfigSource configSource = TestKafkaConfigSourceFactory.createSource(BOOTSTRAP_CONNECT);

        AloKafkaSender.create(configSource)
            .sendValues(Mono.just("DATA"), topic, Function.identity())
            .then()
            .block();

        AloFlux<ConsumerRecord<Object, Object>> aloFlux = AloKafkaReceiver.create(configSource)
            .receiveAloRecords(Collections.singletonList(topic));

        StepVerifier.create(aloFlux)
            .consumeNextWith(Alo::acknowledge)
            .then(() -> aloFlux.unwrap().timeout(Duration.ZERO, Flux.empty()).blockFirst())
            .expectError(IllegalStateException.class)
            .verify(Duration.ofSeconds(30));
    }

    @Test
    public void receivePrioritizedAloRecords_givenBackupOfLowPriorityRecords_expectsReceptionOfHighPriorityRecords() {
        KafkaConfigSource configSource = TestKafkaConfigSourceFactory.createSource(BOOTSTRAP_CONNECT);

        AloKafkaSender<Object, Object> sender = AloKafkaSender.create(configSource);

        Flux.range(0, 10_000)
            .map(it -> new ProducerRecord<Object, Object>(topic, 1, it.toString(), it.toString()))
            .transform(sender::sendRecords)
            .then()
            .block();

        ProducerRecord<Object, Object> priorityRecord = new ProducerRecord<>(topic, 0, "DATA", "DATA");

        AloKafkaReceiver.create(configSource)
            .receivePrioritizedAloRecords(topic, TopicPartition::partition)
            .map(ConsumerRecord::value)
            .consumeAloAndGet(Alo::acknowledge)
            .as(StepVerifier::create)
            .expectNextCount(1_000)
            .then(() -> sender.sendRecord(priorityRecord).block())
            .recordWith(ArrayList::new)
            .expectNextCount(5_000)
            .expectRecordedMatches(it -> it.contains(priorityRecord.value()))
            .thenCancel()
            .verify(Duration.ofSeconds(30));
    }
}