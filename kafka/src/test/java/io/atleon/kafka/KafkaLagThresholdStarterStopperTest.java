package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.kafka.embedded.EmbeddedKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;

class KafkaLagThresholdStarterStopperTest {

    private static final String BOOTSTRAP_CONNECT = EmbeddedKafka.startAndGetBootstrapServersConnect(10092);

    private static final KafkaConfigSource KAFKA_CONFIG_SOURCE = TestKafkaConfigSourceFactory.createSource(BOOTSTRAP_CONNECT);

    private final AloKafkaSender<String, String> sender = AloKafkaSender.create(KAFKA_CONFIG_SOURCE);

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
            .verify(Duration.ofSeconds(30));
    }

    @Test
    public void startStop_givenManyConsumerGroupsAllHaveTotalLagAboveThreshold_expectsStopSignal() {
        String groupId1 = UUID.randomUUID().toString();
        String groupId2 = UUID.randomUUID().toString();
        int threshold = 2;

        produceMessagesToSinglePartition(threshold + 2);

        consumeMessages(groupId1, 1);
        consumeMessages(groupId2, 1);

        KafkaLagThresholdStarterStopper.create(KAFKA_CONFIG_SOURCE, Arrays.asList(groupId1, groupId2))
                .withSampleDelay(Duration.ofMillis(100))
                .withThreshold(threshold)
                .startStop()
                .as(StepVerifier::create)
                .expectNext(false)
                .thenCancel()
                .verify();
    }

    @Test
    public void startStop_givenManyConsumerGroupsOneHasTotalLagAboveThreshold_expectsStopSignal() {
        String groupId1 = UUID.randomUUID().toString();
        String groupId2 = UUID.randomUUID().toString();
        int threshold = 2;

        produceMessagesToSinglePartition(threshold + 2);

        consumeMessages(groupId1, 1);
        consumeMessages(groupId2, threshold + 2);

        KafkaLagThresholdStarterStopper.create(KAFKA_CONFIG_SOURCE, Arrays.asList(groupId1, groupId2))
                .withSampleDelay(Duration.ofMillis(100))
                .withThreshold(threshold)
                .startStop()
                .as(StepVerifier::create)
                .expectNext(false)
                .thenCancel()
                .verify();
    }

    @Test
    public void startStop_givenManyConsumerGroupsAllHaveLagThatGoAboveHighTideThenBackToLowTide_expectsCorrectSignals() {
        String groupId1 = UUID.randomUUID().toString();
        String groupId2 = UUID.randomUUID().toString();
        Duration sampleDelay = Duration.ofMillis(100);
        int highTide = 4;
        int lowTide = highTide - 2;

        produceMessagesToSinglePartition(1); // Ensure topic and partitions exist

        consumeMessages(groupId1, 1); // Ensure group has committed offsets
        consumeMessages(groupId2, 1); // Ensure group has committed offsets

        KafkaLagThresholdStarterStopper.create(KAFKA_CONFIG_SOURCE, Arrays.asList(groupId1, groupId2))
                .withSampleDelay(sampleDelay)
                .withThresholds(highTide, lowTide)
                .startStop()
                .as(StepVerifier::create)
                .expectNext(true) // No lag
                .then(() -> produceMessagesToSinglePartition(highTide / 2)) // Total lag == highTide
                .expectNoEvent(sampleDelay.multipliedBy(2))
                .then(() -> produceMessagesToSinglePartition(1)) // Total lag == highTide + 2
                .expectNext(false)
                .then(() -> consumeMessages(groupId1, 1)) // Total lag == highTide + 1
                .then(() -> consumeMessages(groupId2, 2)) // Total lag == highTide - 1 == lowTide + 1
                .expectNoEvent(sampleDelay.multipliedBy(2))
                .then(() -> consumeMessages(groupId1, 1)) // Total lag == lowTide
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
        KAFKA_CONFIG_SOURCE.withConsumerGroupId(groupId)
            .as(AloKafkaReceiver::<Object, String>create)
            .receiveAloValues(topic)
            .consumeAloAndGet(Alo::acknowledge)
            .take(count)
            .then()
            .block();
    }
}