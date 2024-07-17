package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

class KafkaLagThresholdStarterStopperTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    private static final KafkaConfigSource KAFKA_CONFIG_SOURCE = TestKafkaConfigSourceFactory.createSource(BOOTSTRAP_CONNECT);

    private final AloKafkaSender<String, String> sender = AloKafkaSender.from(KAFKA_CONFIG_SOURCE);

    private final String topic = KafkaLagThresholdStarterStopperTest.class.getSimpleName() + UUID.randomUUID();

    @Test
    public void startStop_givenConsumerGroupIsNotAssociatedWithAnyTopicPartitions_expectsStartSignal() {
        String groupId = UUID.randomUUID().toString();

        KafkaLagThresholdStarterStopper.create(KAFKA_CONFIG_SOURCE, groupId)
            .withSampleDelay(Duration.ofMillis(100))
            .startStop()
            .as(StepVerifier::create)
            .expectNext(true)
            .thenCancel()
            .verify();
    }

    @Test
    public void startStop_givenConsumerGroupHasLagAboveThreshold_expectsStopSignal() {
        String groupId = UUID.randomUUID().toString();
        int threshold = 2;

        produceMessagesToSinglePartition(threshold + 2);

        consumeMessages(groupId, 1);

        KafkaLagThresholdStarterStopper.create(KAFKA_CONFIG_SOURCE, groupId)
            .withSampleDelay(Duration.ofMillis(100))
            .withThreshold(threshold)
            .startStop()
            .as(StepVerifier::create)
            .expectNext(false)
            .thenCancel()
            .verify();
    }

    @Test
    public void startStop_givenConsumerGroupHasLagThatGoesAboveHighTideThenBackToLowTide_expectsCorrectSignals() {
        String groupId = UUID.randomUUID().toString();
        Duration sampleDelay = Duration.ofMillis(100);
        int highTide = 4;
        int lowTide = highTide - 2;

        produceMessagesToSinglePartition(1); // Ensure topic and partitions exist

        consumeMessages(groupId, 1); // Ensure group has committed offsets

        KafkaLagThresholdStarterStopper.create(KAFKA_CONFIG_SOURCE, groupId)
            .withSampleDelay(sampleDelay)
            .withThresholds(highTide, lowTide)
            .startStop()
            .as(StepVerifier::create)
            .expectNext(true) // No lag
            .then(() -> produceMessagesToSinglePartition(highTide)) // Total lag == highTide
            .expectNoEvent(sampleDelay.multipliedBy(2))
            .then(() -> produceMessagesToSinglePartition(1)) // Total lag == highTide + 1
            .expectNext(false)
            .then(() -> consumeMessages(groupId, 2)) // Total lag == highTide - 1 == lowTide + 1
            .expectNoEvent(sampleDelay.multipliedBy(2))
            .then(() -> consumeMessages(groupId, 1)) // Total lag == lowTide
            .expectNext(true)
            .thenCancel()
            .verify(Duration.ofSeconds(30));
    }

    private void produceMessagesToSinglePartition(int numMessages) {
        Flux.range(0, numMessages)
            .map(partition -> new ProducerRecord<>(topic, 0, null, "key", "value"))
            .transform(sender::sendRecords)
            .then()
            .block();
    }

    private void consumeMessages(String groupId, int count) {
        AloKafkaReceiver.<String>forValues(KAFKA_CONFIG_SOURCE.withConsumerGroupId(groupId))
            .receiveAloValues(topic)
            .consumeAloAndGet(Alo::acknowledge)
            .take(count)
            .then()
            .block();
    }
}