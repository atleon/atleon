package io.atleon.kafka;

import io.atleon.core.AcknowledgementQueueMode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class ActivePartitionTest {

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 0);

    @Test
    public void activateForProcessing_givenActive_expectsRegisteredReceiverRecord() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition.acknowledgedOffsets().subscribe(acknowledgedOffsets::add);
        activePartition.deactivatedRecordCounts().subscribe(deactivatedRecordCounts::add);

        Optional<KafkaReceiverRecord<String, String>> activated =
                activePartition.activateForProcessing(newConsumerRecord(0));

        assertTrue(activated.isPresent());
        assertTrue(acknowledgedOffsets.isEmpty());
        assertTrue(deactivatedRecordCounts.isEmpty());
    }

    @Test
    public void activateForProcessing_givenDeactivated_expectsNoReceiverRecord() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition.acknowledgedOffsets().subscribe(acknowledgedOffsets::add);
        activePartition.deactivatedRecordCounts().subscribe(deactivatedRecordCounts::add);

        Long deactivatedRecordCount = activePartition.deactivateForcefully().block();

        Optional<KafkaReceiverRecord<String, String>> activated =
                activePartition.activateForProcessing(newConsumerRecord(0));

        assertEquals(0L, deactivatedRecordCount);
        assertFalse(activated.isPresent());
        assertTrue(acknowledgedOffsets.isEmpty());
        assertTrue(deactivatedRecordCounts.isEmpty());
    }

    @Test
    public void acknowledge_givenActivatedRecordIsAcknowledged_expectsCorrectAcknowledgements() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition.acknowledgedOffsets().subscribe(acknowledgedOffsets::add);
        activePartition.deactivatedRecordCounts().subscribe(deactivatedRecordCounts::add);

        ConsumerRecord<String, String> consumerRecord = newConsumerRecord(0);
        activePartition.activateForProcessing(consumerRecord).ifPresent(KafkaReceiverRecord::acknowledge);

        assertEquals(1, acknowledgedOffsets.size());
        assertEquals(TOPIC_PARTITION, acknowledgedOffsets.get(0).topicPartition());
        assertEquals(
                consumerRecord.offset() + 1,
                acknowledgedOffsets.get(0).nextOffsetAndMetadata().offset());
        assertEquals(Collections.singletonList(1L), deactivatedRecordCounts);
    }

    @Test
    public void acknowledge_givenMultipleActivatedRecordsAreAcknowledged_expectsCorrectAcknowledgements() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition.acknowledgedOffsets().subscribe(acknowledgedOffsets::add);
        activePartition.deactivatedRecordCounts().subscribe(deactivatedRecordCounts::add);

        KafkaReceiverRecord<String, String> receiverRecord1 =
                activePartition.activateForProcessing(newConsumerRecord(0)).get();
        KafkaReceiverRecord<String, String> receiverRecord2 =
                activePartition.activateForProcessing(newConsumerRecord(1)).get();
        KafkaReceiverRecord<String, String> receiverRecord3 =
                activePartition.activateForProcessing(newConsumerRecord(2)).get();

        receiverRecord3.acknowledge();
        receiverRecord1.acknowledge();
        receiverRecord2.acknowledge();

        assertEquals(3, acknowledgedOffsets.size());
        assertEquals(TOPIC_PARTITION, acknowledgedOffsets.get(0).topicPartition());
        assertEquals(TOPIC_PARTITION, acknowledgedOffsets.get(1).topicPartition());
        assertEquals(TOPIC_PARTITION, acknowledgedOffsets.get(2).topicPartition());
        assertEquals(
                receiverRecord1.consumerRecord().offset() + 1,
                acknowledgedOffsets.get(0).nextOffsetAndMetadata().offset());
        assertEquals(
                receiverRecord2.consumerRecord().offset() + 1,
                acknowledgedOffsets.get(1).nextOffsetAndMetadata().offset());
        assertEquals(
                receiverRecord3.consumerRecord().offset() + 1,
                acknowledgedOffsets.get(2).nextOffsetAndMetadata().offset());
        assertEquals(Arrays.asList(1L, 2L), deactivatedRecordCounts);
    }

    @Test
    public void nacknowledge_givenActivatedRecordIsNacknowledged_expectsErrorEmission() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition.acknowledgedOffsets().subscribe(acknowledgedOffsets::add, acknowledgedOffsetsError::set);
        activePartition
                .deactivatedRecordCounts()
                .subscribe(
                        deactivatedRecordCounts::add,
                        deactivatedRecordCountsError::set,
                        () -> deactivatedRecordCountsCompleted.set(true));

        ConsumerRecord<String, String> consumerRecord = newConsumerRecord(0);
        activePartition
                .activateForProcessing(consumerRecord)
                .ifPresent(it -> it.nacknowledge(new IllegalStateException("Boom")));

        assertTrue(acknowledgedOffsets.isEmpty());
        assertInstanceOf(IllegalStateException.class, acknowledgedOffsetsError.get());
        assertEquals(Collections.singletonList(1L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void nacknowledge_givenMultipleActivatedRecordsAreNacknowledged_expectsCorrectEmissions() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition.acknowledgedOffsets().subscribe(acknowledgedOffsets::add, acknowledgedOffsetsError::set);
        activePartition
                .deactivatedRecordCounts()
                .subscribe(
                        deactivatedRecordCounts::add,
                        deactivatedRecordCountsError::set,
                        () -> deactivatedRecordCountsCompleted.set(true));

        KafkaReceiverRecord<String, String> receiverRecord1 =
                activePartition.activateForProcessing(newConsumerRecord(0)).get();
        KafkaReceiverRecord<String, String> receiverRecord2 =
                activePartition.activateForProcessing(newConsumerRecord(1)).get();
        KafkaReceiverRecord<String, String> receiverRecord3 =
                activePartition.activateForProcessing(newConsumerRecord(2)).get();
        KafkaReceiverRecord<String, String> receiverRecord4 =
                activePartition.activateForProcessing(newConsumerRecord(2)).get();

        receiverRecord3.acknowledge();
        receiverRecord4.nacknowledge(new UnsupportedOperationException("Boom"));
        receiverRecord1.acknowledge();
        receiverRecord2.nacknowledge(new IllegalStateException("Boom"));

        assertEquals(1, acknowledgedOffsets.size());
        assertEquals(
                receiverRecord1.consumerRecord().offset() + 1,
                acknowledgedOffsets.get(0).nextOffsetAndMetadata().offset());
        assertInstanceOf(IllegalStateException.class, acknowledgedOffsetsError.get());
        assertEquals(Arrays.asList(1L, 3L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void deactivateLatest_givenNoInFlightRecords_expectsTerminationWithLastAcknowledgedOffset() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        AtomicBoolean acknowledgedOffsetsCompleted = new AtomicBoolean(false);
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition
                .acknowledgedOffsets()
                .subscribe(
                        acknowledgedOffsets::add,
                        acknowledgedOffsetsError::set,
                        () -> acknowledgedOffsetsCompleted.set(true));
        activePartition
                .deactivatedRecordCounts()
                .subscribe(
                        deactivatedRecordCounts::add,
                        deactivatedRecordCountsError::set,
                        () -> deactivatedRecordCountsCompleted.set(true));

        KafkaReceiverRecord<String, String> receiverRecord =
                activePartition.activateForProcessing(newConsumerRecord(0)).get();

        receiverRecord.acknowledge();

        AcknowledgedOffset lastAcknowledgedOffset = activePartition
                .deactivateLatest(Duration.ofDays(1), Schedulers.parallel(), Mono.never())
                .block();

        assertNotNull(lastAcknowledgedOffset);
        assertEquals(Collections.singletonList(lastAcknowledgedOffset), acknowledgedOffsets);
        assertNull(acknowledgedOffsetsError.get());
        assertTrue(acknowledgedOffsetsCompleted.get());
        assertEquals(Collections.singletonList(1L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void deactivateLatest_givenGracePeriod_expectsTerminationAfterLastAcknowledgement() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        AtomicBoolean acknowledgedOffsetsCompleted = new AtomicBoolean(false);
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition
                .acknowledgedOffsets()
                .subscribe(
                        acknowledgedOffsets::add,
                        acknowledgedOffsetsError::set,
                        () -> acknowledgedOffsetsCompleted.set(true));
        activePartition
                .deactivatedRecordCounts()
                .subscribe(
                        deactivatedRecordCounts::add,
                        deactivatedRecordCountsError::set,
                        () -> deactivatedRecordCountsCompleted.set(true));

        KafkaReceiverRecord<String, String> receiverRecord1 =
                activePartition.activateForProcessing(newConsumerRecord(0)).get();
        KafkaReceiverRecord<String, String> receiverRecord2 =
                activePartition.activateForProcessing(newConsumerRecord(0)).get();

        AtomicReference<AcknowledgedOffset> lastAcknowledgedOffset = new AtomicReference<>();
        activePartition
                .deactivateLatest(Duration.ofDays(1), Schedulers.parallel(), Mono.never())
                .subscribe(lastAcknowledgedOffset::set);

        assertTrue(acknowledgedOffsets.isEmpty());
        assertTrue(deactivatedRecordCounts.isEmpty());

        receiverRecord1.acknowledge();
        receiverRecord2.acknowledge();

        assertNotNull(lastAcknowledgedOffset.get());
        assertEquals(2, acknowledgedOffsets.size());
        assertEquals(
                receiverRecord1.consumerRecord().offset() + 1,
                acknowledgedOffsets.get(0).nextOffsetAndMetadata().offset());
        assertEquals(
                receiverRecord2.consumerRecord().offset() + 1,
                acknowledgedOffsets.get(1).nextOffsetAndMetadata().offset());
        assertEquals(lastAcknowledgedOffset.get(), acknowledgedOffsets.get(1));
        assertNull(acknowledgedOffsetsError.get());
        assertTrue(acknowledgedOffsetsCompleted.get());
        assertEquals(Arrays.asList(1L, 1L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void deactivateLatest_givenManualTimeout_expectsImmediateTerminationWithLastAcknowledgedOffset() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        AtomicBoolean acknowledgedOffsetsCompleted = new AtomicBoolean(false);
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition
                .acknowledgedOffsets()
                .subscribe(
                        acknowledgedOffsets::add,
                        acknowledgedOffsetsError::set,
                        () -> acknowledgedOffsetsCompleted.set(true));
        activePartition
                .deactivatedRecordCounts()
                .subscribe(
                        deactivatedRecordCounts::add,
                        deactivatedRecordCountsError::set,
                        () -> deactivatedRecordCountsCompleted.set(true));

        KafkaReceiverRecord<String, String> receiverRecord1 =
                activePartition.activateForProcessing(newConsumerRecord(0)).get();
        KafkaReceiverRecord<String, String> receiverRecord2 =
                activePartition.activateForProcessing(newConsumerRecord(1)).get();

        Sinks.Empty<Void> forcedTimeout = Sinks.one();
        AtomicReference<AcknowledgedOffset> lastAcknowledgedOffset = new AtomicReference<>();
        activePartition
                .deactivateLatest(Duration.ofDays(1), Schedulers.parallel(), forcedTimeout.asMono())
                .subscribe(lastAcknowledgedOffset::set);
        receiverRecord1.acknowledge();
        forcedTimeout.tryEmitEmpty();
        receiverRecord2.acknowledge();

        assertNotNull(lastAcknowledgedOffset.get());
        assertEquals(Collections.singletonList(lastAcknowledgedOffset.get()), acknowledgedOffsets);
        assertNull(acknowledgedOffsetsError.get());
        assertTrue(acknowledgedOffsetsCompleted.get());
        assertEquals(Arrays.asList(1L, 1L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void deactivateLatest_givenNoGracePeriod_expectsImmediateTerminationWithLastAcknowledgedOffset() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        AtomicBoolean acknowledgedOffsetsCompleted = new AtomicBoolean(false);
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition
                .acknowledgedOffsets()
                .subscribe(
                        acknowledgedOffsets::add,
                        acknowledgedOffsetsError::set,
                        () -> acknowledgedOffsetsCompleted.set(true));
        activePartition
                .deactivatedRecordCounts()
                .subscribe(
                        deactivatedRecordCounts::add,
                        deactivatedRecordCountsError::set,
                        () -> deactivatedRecordCountsCompleted.set(true));

        KafkaReceiverRecord<String, String> receiverRecord1 =
                activePartition.activateForProcessing(newConsumerRecord(0)).get();
        KafkaReceiverRecord<String, String> receiverRecord2 =
                activePartition.activateForProcessing(newConsumerRecord(1)).get();
        KafkaReceiverRecord<String, String> receiverRecord3 =
                activePartition.activateForProcessing(newConsumerRecord(2)).get();

        receiverRecord3.acknowledge();
        receiverRecord1.acknowledge();

        AtomicReference<AcknowledgedOffset> lastAcknowledgedOffset = new AtomicReference<>();
        activePartition
                .deactivateLatest(Duration.ZERO, Schedulers.parallel(), Mono.never())
                .subscribe(lastAcknowledgedOffset::set);
        receiverRecord2.acknowledge();

        assertNotNull(lastAcknowledgedOffset.get());
        assertEquals(Collections.singletonList(lastAcknowledgedOffset.get()), acknowledgedOffsets);
        assertNull(acknowledgedOffsetsError.get());
        assertTrue(acknowledgedOffsetsCompleted.get());
        assertEquals(Arrays.asList(1L, 2L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void deactivateTimeout_givenNoGracePeriodAndNothingInFlight_expectsSuccessfulCompletion() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        AtomicBoolean acknowledgedOffsetsCompleted = new AtomicBoolean(false);
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition
                .acknowledgedOffsets()
                .subscribe(
                        acknowledgedOffsets::add,
                        acknowledgedOffsetsError::set,
                        () -> acknowledgedOffsetsCompleted.set(true));
        activePartition
                .deactivatedRecordCounts()
                .subscribe(
                        deactivatedRecordCounts::add,
                        deactivatedRecordCountsError::set,
                        () -> deactivatedRecordCountsCompleted.set(true));

        activePartition
                .deactivateTimeout(Duration.ofSeconds(-1), Schedulers.immediate())
                .block();

        assertTrue(acknowledgedOffsets.isEmpty());
        assertNull(acknowledgedOffsetsError.get());
        assertTrue(acknowledgedOffsetsCompleted.get());
        assertTrue(deactivatedRecordCounts.isEmpty());
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void deactivateForcefully_givenInFlightRecords_expectsImmediateTermination() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        CountDownLatch acknowledgedOffsetsCompletionLatch = new CountDownLatch(1);
        CompletableFuture<Void> acknowledgedOffsetsCompletion = new CompletableFuture<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        CompletableFuture<Void> deactivatedRecordCountsCompletion = new CompletableFuture<>();

        ActivePartition activePartition = new ActivePartition(TOPIC_PARTITION, AcknowledgementQueueMode.STRICT);
        activePartition
                .acknowledgedOffsets()
                .publishOn(Schedulers.boundedElastic())
                .subscribe(acknowledgedOffsets::add, acknowledgedOffsetsError::set, () -> {
                    try {
                        acknowledgedOffsetsCompletionLatch.await();
                    } catch (InterruptedException e) {
                        fail("Interrupted while awaiting acknowledged offsets completion");
                    }
                    acknowledgedOffsetsCompletion.complete(null);
                });
        activePartition
                .deactivatedRecordCounts()
                .subscribe(
                        deactivatedRecordCounts::add,
                        deactivatedRecordCountsError::set,
                        () -> deactivatedRecordCountsCompletion.complete(null));

        KafkaReceiverRecord<String, String> receiverRecord1 =
                activePartition.activateForProcessing(newConsumerRecord(0)).get();
        KafkaReceiverRecord<String, String> receiverRecord2 =
                activePartition.activateForProcessing(newConsumerRecord(1)).get();
        KafkaReceiverRecord<String, String> receiverRecord3 =
                activePartition.activateForProcessing(newConsumerRecord(2)).get();

        receiverRecord3.acknowledge();
        receiverRecord1.acknowledge();

        assertEquals(2L, activePartition.deactivateForcefully().block());

        receiverRecord2.acknowledge();
        acknowledgedOffsetsCompletionLatch.countDown();
        acknowledgedOffsetsCompletion.join();
        deactivatedRecordCountsCompletion.join();

        assertEquals(TOPIC_PARTITION, activePartition.topicPartition());
        assertEquals(1, acknowledgedOffsets.size());
        assertEquals(
                receiverRecord1.consumerRecord().offset() + 1,
                acknowledgedOffsets.get(0).nextOffsetAndMetadata().offset());
        assertNull(acknowledgedOffsetsError.get());
        assertEquals(Arrays.asList(1L, 2L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
    }

    private static ConsumerRecord<String, String> newConsumerRecord(int offset) {
        return new ConsumerRecord<>(TOPIC_PARTITION.topic(), TOPIC_PARTITION.partition(), offset, "key", "value");
    }
}
