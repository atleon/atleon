package io.atleon.kafka;

import io.atleon.core.AcknowledgementQueueMode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalAnswers;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ActivePartitionTest {

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("topic", 0);

    @Test
    public void activateForProcessing_givenActive_expectsRegisteredReceiverRecord() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
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

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
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
    public void activateForProcessing_givenOffsetTrackerProhibitsProcessing_expectsRecordAcknowledgedWithoutEmission() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(prohibitingOffsetTracker(1L), AcknowledgementQueueMode.STRICT);
        activePartition.acknowledgedOffsets().subscribe(acknowledgedOffsets::add);
        activePartition.deactivatedRecordCounts().subscribe(deactivatedRecordCounts::add);

        ConsumerRecord<String, String> allowedRecord = newConsumerRecord(0);
        ConsumerRecord<String, String> prohibitedRecord = newConsumerRecord(1);
        Optional<KafkaReceiverRecord<String, String>> allowed = activePartition.activateForProcessing(allowedRecord);
        Optional<KafkaReceiverRecord<String, String>> prohibited =
                activePartition.activateForProcessing(prohibitedRecord);

        assertFalse(prohibited.isPresent());
        assertTrue(allowed.isPresent());
        assertTrue(acknowledgedOffsets.isEmpty());
        assertTrue(deactivatedRecordCounts.isEmpty());

        allowed.get().acknowledge();
        assertEquals(2, acknowledgedOffsets.size());
        assertEquals(Collections.singletonList(2L), deactivatedRecordCounts);
        assertEquals(
                allowedRecord.offset(),
                acknowledgedOffsets.get(0).consumerOffset().offset());
        assertEquals(
                prohibitedRecord.offset(),
                acknowledgedOffsets.get(1).consumerOffset().offset());
    }

    @Test
    public void acknowledgedOffsets_givenInitiallyCommittableOffset_expectsAcknowledgedOffsetWithUpdatedMetadata() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();

        ConsumerOffset initialConsumerOffset = new ConsumerOffset(TOPIC_PARTITION, 4L);
        ActivePartition<String, String> activePartition =
                new ActivePartition<>(initializedOffsetTracker(initialConsumerOffset), AcknowledgementQueueMode.STRICT);
        activePartition.acknowledgedOffsets().subscribe(acknowledgedOffsets::add);

        // Even without any record being acknowledged, an initially committable offset is emitted as
        // an acknowledged offset eligible for (re)commitment. Its underlying consumer offset is one
        // less than the committable offset, since the committed offset is the numerical increment.
        assertEquals(1, acknowledgedOffsets.size());
        assertEquals(
                initialConsumerOffset.offset(),
                acknowledgedOffsets.get(0).consumerOffset().offset());

        // On commitment, the offset tracker supplies updated metadata for the committable offset.
        OffsetAndMetadata committed =
                acknowledgedOffsets.get(0).prepareCommitment().block().getT2();
        assertEquals(initialConsumerOffset.offset() + 1, committed.offset());
        assertEquals("metadata-" + (initialConsumerOffset.offset() + 1), committed.metadata());
    }

    @Test
    public void acknowledge_givenActivatedRecordIsAcknowledged_expectsCorrectAcknowledgements() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
        activePartition.acknowledgedOffsets().subscribe(acknowledgedOffsets::add);
        activePartition.deactivatedRecordCounts().subscribe(deactivatedRecordCounts::add);

        ConsumerRecord<String, String> consumerRecord = newConsumerRecord(0);
        activePartition.activateForProcessing(consumerRecord).ifPresent(KafkaReceiverRecord::acknowledge);

        assertEquals(1, acknowledgedOffsets.size());
        assertEquals(TOPIC_PARTITION, acknowledgedOffsets.get(0).topicPartition());
        assertEquals(
                consumerRecord.offset() + 1,
                acknowledgedOffsets.get(0).prepareCommitment().block().getT2().offset());
        assertEquals(Collections.singletonList(1L), deactivatedRecordCounts);
    }

    @Test
    public void acknowledge_givenMultipleActivatedRecordsAreAcknowledged_expectsCorrectAcknowledgements() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
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
                acknowledgedOffsets.get(0).prepareCommitment().block().getT2().offset());
        assertEquals(
                receiverRecord2.consumerRecord().offset() + 1,
                acknowledgedOffsets.get(1).prepareCommitment().block().getT2().offset());
        assertEquals(
                receiverRecord3.consumerRecord().offset() + 1,
                acknowledgedOffsets.get(2).prepareCommitment().block().getT2().offset());
        assertEquals(Arrays.asList(1L, 2L), deactivatedRecordCounts);
    }

    @Test
    public void nacknowledge_givenActivatedRecordIsNacknowledged_expectsErrorEmission() {
        List<AcknowledgedOffset> acknowledgedOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
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

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
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
                acknowledgedOffsets.get(0).prepareCommitment().block().getT2().offset());
        assertInstanceOf(IllegalStateException.class, acknowledgedOffsetsError.get());
        assertEquals(Arrays.asList(1L, 3L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void deactivateLatest_givenNoInFlightRecords_expectsTerminationWithLastAcknowledgedOffset() {
        List<ConsumerOffset> acknowledgedConsumerOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        AtomicBoolean acknowledgedOffsetsCompleted = new AtomicBoolean(false);
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
        activePartition
                .acknowledgedOffsets()
                .subscribe(
                        it -> acknowledgedConsumerOffsets.add(it.consumerOffset()),
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
        assertEquals(Collections.singletonList(lastAcknowledgedOffset.consumerOffset()), acknowledgedConsumerOffsets);
        assertNull(acknowledgedOffsetsError.get());
        assertTrue(acknowledgedOffsetsCompleted.get());
        assertEquals(Collections.singletonList(1L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void deactivateLatest_givenGracePeriod_expectsTerminationAfterLastAcknowledgement() {
        List<ConsumerOffset> acknowledgedConsumerOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        AtomicBoolean acknowledgedOffsetsCompleted = new AtomicBoolean(false);
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
        activePartition
                .acknowledgedOffsets()
                .subscribe(
                        it -> acknowledgedConsumerOffsets.add(it.consumerOffset()),
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

        assertTrue(acknowledgedConsumerOffsets.isEmpty());
        assertTrue(deactivatedRecordCounts.isEmpty());

        receiverRecord1.acknowledge();
        receiverRecord2.acknowledge();

        assertNotNull(lastAcknowledgedOffset.get());
        assertEquals(2, acknowledgedConsumerOffsets.size());
        assertEquals(
                receiverRecord1.consumerRecord().offset(),
                acknowledgedConsumerOffsets.get(0).offset());
        assertEquals(
                receiverRecord2.consumerRecord().offset(),
                acknowledgedConsumerOffsets.get(1).offset());
        assertEquals(lastAcknowledgedOffset.get().consumerOffset(), acknowledgedConsumerOffsets.get(1));
        assertNull(acknowledgedOffsetsError.get());
        assertTrue(acknowledgedOffsetsCompleted.get());
        assertEquals(Arrays.asList(1L, 1L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void deactivateLatest_givenManualTimeout_expectsImmediateTerminationWithLastAcknowledgedOffset() {
        List<ConsumerOffset> acknowledgedConsumerOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        AtomicBoolean acknowledgedOffsetsCompleted = new AtomicBoolean(false);
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
        activePartition
                .acknowledgedOffsets()
                .subscribe(
                        it -> acknowledgedConsumerOffsets.add(it.consumerOffset()),
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
        assertEquals(
                Collections.singletonList(lastAcknowledgedOffset.get().consumerOffset()), acknowledgedConsumerOffsets);
        assertNull(acknowledgedOffsetsError.get());
        assertTrue(acknowledgedOffsetsCompleted.get());
        assertEquals(Arrays.asList(1L, 1L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
        assertTrue(deactivatedRecordCountsCompleted.get());
    }

    @Test
    public void deactivateLatest_givenNoGracePeriod_expectsImmediateTerminationWithLastAcknowledgedOffset() {
        List<ConsumerOffset> acknowledgedConsumerOffsets = new ArrayList<>();
        AtomicReference<Throwable> acknowledgedOffsetsError = new AtomicReference<>();
        AtomicBoolean acknowledgedOffsetsCompleted = new AtomicBoolean(false);
        List<Long> deactivatedRecordCounts = new ArrayList<>();
        AtomicReference<Throwable> deactivatedRecordCountsError = new AtomicReference<>();
        AtomicBoolean deactivatedRecordCountsCompleted = new AtomicBoolean(false);

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
        activePartition
                .acknowledgedOffsets()
                .subscribe(
                        it -> acknowledgedConsumerOffsets.add(it.consumerOffset()),
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
        assertEquals(
                Collections.singletonList(lastAcknowledgedOffset.get().consumerOffset()), acknowledgedConsumerOffsets);
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

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
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

        ActivePartition<String, String> activePartition =
                new ActivePartition<>(newOffsetTracker(), AcknowledgementQueueMode.STRICT);
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
                acknowledgedOffsets.get(0).prepareCommitment().block().getT2().offset());
        assertNull(acknowledgedOffsetsError.get());
        assertEquals(Arrays.asList(1L, 2L), deactivatedRecordCounts);
        assertNull(deactivatedRecordCountsError.get());
    }

    private static ConsumerRecord<String, String> newConsumerRecord(int offset) {
        return new ConsumerRecord<>(TOPIC_PARTITION.topic(), TOPIC_PARTITION.partition(), offset, "key", "value");
    }

    private static OffsetTracker prohibitingOffsetTracker(Long... prohibitedOffsets) {
        Set<Long> prohibited = Stream.of(prohibitedOffsets).collect(Collectors.toSet());
        OffsetTracker offsetTracker = mock(OffsetTracker.class, AdditionalAnswers.delegatesTo(newOffsetTracker()));
        when(offsetTracker.prohibitsProcessing(anyLong())).thenAnswer(invocation -> {
            Long offset = invocation.getArgument(0);
            return prohibited.contains(offset);
        });
        return offsetTracker;
    }

    private static OffsetTracker initializedOffsetTracker(ConsumerOffset initialConsumerOffset) {
        OffsetTracker offsetTracker = mock(OffsetTracker.class, AdditionalAnswers.delegatesTo(newOffsetTracker()));
        when(offsetTracker.initialConsumerOffset()).thenReturn(Optional.of(initialConsumerOffset));
        when(offsetTracker.commitMetadata(anyLong()))
                .thenAnswer(invocation -> Mono.just("metadata-" + invocation.<Long>getArgument(0)));
        return offsetTracker;
    }

    private static OffsetTracker newOffsetTracker() {
        return OffsetTracker.simple(TOPIC_PARTITION);
    }
}
