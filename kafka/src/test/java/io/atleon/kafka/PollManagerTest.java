package io.atleon.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PollManagerTest {

    @Test
    public void activateAssigned_givenAssignedPartitions_expectsCoordinationOfInvocations() {
        TopicPartition partition1 = new TopicPartition("topic", 0);
        TopicPartition partition2 = new TopicPartition("topic", 1);

        Consumer<?, ?> consumer = Mockito.mock(Consumer.class);
        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);
        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 1, Duration.ZERO);
        pollManager.forcePause(Collections.singletonList(partition1));

        pollManager.activateAssigned(consumer, Arrays.asList(partition1, partition2), Function.identity());

        verify(consumer).pause(argThat(it -> it.size() == 1 && it.contains(partition1)));
        verify(pollStrategy).onPollingPermitted(argThat(it -> it.size() == 1 && it.contains(partition2)));
        assertEquals(partition1, pollManager.activated(partition1));
        assertEquals(partition2, pollManager.activated(partition2));
    }

    @Test
    public void activateAssigned_givenAlreadyAssignedPartition_expectsRetainment() {
        TopicPartition original = new TopicPartition("topic", 0);
        TopicPartition duplicate = new TopicPartition("topic", 0);
        Consumer<?, ?> consumer = Mockito.mock(Consumer.class);
        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);
        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 1, Duration.ZERO);

        pollManager.activateAssigned(consumer, Collections.singletonList(original), Function.identity());

        pollManager.activateAssigned(consumer, Collections.singletonList(duplicate), Function.identity());

        assertSame(original, pollManager.activated(original));
        assertSame(original, pollManager.activated(duplicate));
    }

    @Test
    public void unassigned_givenNonAssignedPartition_expectsEmptyResult() {
        TopicPartition partition = new TopicPartition("topic", 0);
        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);
        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 1, Duration.ZERO);

        Collection<TopicPartition> result = pollManager.unassigned(Collections.singletonList(partition));

        assertTrue(result.isEmpty());
    }

    @Test
    public void unassigned_givenRevokedPartition_expectsCoordinationOfInvocations() {
        TopicPartition partition1 = new TopicPartition("topic", 0);
        TopicPartition partition2 = new TopicPartition("topic", 1);

        Consumer<?, ?> consumer = Mockito.mock(Consumer.class);
        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);
        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 1, Duration.ZERO);
        pollManager.activateAssigned(consumer, Arrays.asList(partition1, partition2), Function.identity());

        Collection<TopicPartition> result = pollManager.unassigned(Collections.singletonList(partition1));

        assertEquals(1, result.size());
        assertTrue(result.contains(partition1));
        verify(pollStrategy).onPollingProhibited(argThat(it -> it.size() == 1 && it.contains(partition1)));
        assertEquals(partition2, pollManager.activated(partition2));
    }

    @Test
    public void pollWakeably_givenSufficientCapacity_expectsPreparationAndPolling() {
        Consumer<?, ?> consumer = Mockito.mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(ConsumerRecords.empty());

        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);

        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 1, Duration.ZERO);

        ConsumerRecords<?, ?> records = pollManager.pollWakeably(consumer, () -> 1, () -> true);

        assertEquals(ConsumerRecords.empty(), records);
        verify(pollStrategy).prepareForPoll(any());
        verify(consumer, never()).pause(any());
        verify(consumer).poll(any());
    }

    @Test
    public void pollWakeably_givenInsufficientCapacity_expectsPreparationAndPolling() {
        Consumer<?, ?> consumer = Mockito.mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(ConsumerRecords.empty());

        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);

        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 1, Duration.ZERO);

        ConsumerRecords<?, ?> records = pollManager.pollWakeably(consumer, () -> 0, () -> true);

        assertEquals(ConsumerRecords.empty(), records);
        verify(pollStrategy, never()).prepareForPoll(any());
        verify(consumer).pause(any());
        verify(consumer).poll(any());
    }

    @Test
    public void pollWakeably_givenWakeupException_expectsEmptyRecords() {
        Consumer<?, ?> consumer = Mockito.mock(Consumer.class);
        when(consumer.poll(any())).thenThrow(new WakeupException());

        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);
        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 1, Duration.ZERO);

        ConsumerRecords<?, ?> records = pollManager.pollWakeably(consumer, () -> 1, () -> false);

        assertEquals(ConsumerRecords.empty(), records);
        verify(consumer).poll(any());
    }

    @Test
    public void pollWakeably_givenWakeupExceptionAndCanRetry_expectsRecursiveCall() {
        Consumer<?, ?> consumer = Mockito.mock(Consumer.class);
        when(consumer.poll(any()))
            .thenThrow(new WakeupException())
            .thenReturn(ConsumerRecords.empty());

        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);
        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 1, Duration.ZERO);

        ConsumerRecords<?, ?> records = pollManager.pollWakeably(consumer, () -> 1, () -> true);

        assertEquals(ConsumerRecords.empty(), records);
        verify(consumer, times(2)).poll(any());
    }

    @Test
    public void pollWakeably_givenBackpressureTransition_expectsStateUpdate() {
        TopicPartition partition = new TopicPartition("topic", 0);
        Consumer<?, ?> consumer = Mockito.mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(ConsumerRecords.empty());

        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);
        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 2, Duration.ZERO);
        pollManager.activateAssigned(consumer, Collections.singletonList(partition), Function.identity());

        // First call with sufficient capacity
        pollManager.pollWakeably(consumer, () -> 2, () -> true);
        verify(consumer, never()).pause(any());

        // Second call with insufficient capacity - should pause
        pollManager.pollWakeably(consumer, () -> 1, () -> true);
        verify(consumer).pause(argThat(partitions -> partitions.contains(partition)));
    }

    @Test
    public void shouldWakeupOnSingularCapacityReclamation_givenCorrectConditions_expectsTrue() {
        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);
        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 5, Duration.ZERO);

        // Simulate being paused due to backpressure
        Consumer<?, ?> consumer = Mockito.mock(Consumer.class);
        when(consumer.poll(any())).thenReturn(ConsumerRecords.empty());
        pollManager.pollWakeably(consumer, () -> 1, () -> true);

        assertTrue(pollManager.shouldWakeupOnSingularCapacityReclamation(5));
        assertFalse(pollManager.shouldWakeupOnSingularCapacityReclamation(4));
    }

    @Test
    public void forcePause_givenPartitions_expectsCoordination() {
        TopicPartition partition = new TopicPartition("topic", 0);
        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);
        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 1, Duration.ZERO);

        pollManager.forcePause(Collections.singletonList(partition));

        verify(pollStrategy).onPollingProhibited(argThat(partitions -> partitions.contains(partition)));
    }

    @Test
    public void allowResumption_givenPartitions_expectsCoordination() {
        TopicPartition partition = new TopicPartition("topic", 0);
        PollStrategy pollStrategy = Mockito.mock(PollStrategy.class, Mockito.CALLS_REAL_METHODS);
        PollManager<TopicPartition> pollManager = new PollManager<>(pollStrategy, 1, Duration.ZERO);

        pollManager.forcePause(Collections.singletonList(partition));
        pollManager.allowResumption(Collections.singletonList(partition));

        verify(pollStrategy).onPollingPermitted(argThat(partitions -> partitions.contains(partition)));
    }
}