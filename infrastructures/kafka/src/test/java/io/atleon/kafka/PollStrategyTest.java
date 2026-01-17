package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PollStrategyTest {

    @Test
    public void prepareForPoll_givenConsecutiveBinaryStridesPreparation_expectsStridedPauseResume() {
        List<TopicPartition> partitions = IntStream.range(0, 7)
                .mapToObj(it -> new TopicPartition("topic", it))
                .collect(Collectors.toList());

        PollSelectionContext context = mock(PollSelectionContext.class);

        PollStrategy pollStrategy = PollStrategy.binaryStrides();
        pollStrategy.onPollingPermitted(partitions);

        pollStrategy.prepareForPoll(context);
        verify(context).selectExclusively(eqSelection(partitions, 0, 2, 4, 6));

        pollStrategy.prepareForPoll(context);
        verify(context).selectExclusively(eqSelection(partitions, 1, 3, 5));

        pollStrategy.prepareForPoll(context);
        verify(context).selectExclusively(eqSelection(partitions, 0, 1, 4, 5));

        pollStrategy.prepareForPoll(context);
        verify(context).selectExclusively(eqSelection(partitions, 2, 3, 6));

        pollStrategy.prepareForPoll(context);
        verify(context).selectExclusively(eqSelection(partitions, 0, 1, 2, 3));

        pollStrategy.prepareForPoll(context);
        verify(context).selectExclusively(eqSelection(partitions, 4, 5, 6));
    }

    @Test
    public void prepareForPoll_givenPartitionsWithBatchLag_expectsSelectionOfPartitionsWithHighestBatchLag() {
        List<TopicPartition> topicPartitions = Arrays.asList(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1),
                new TopicPartition("topic", 2),
                new TopicPartition("topic", 3));

        Map<TopicPartition, Long> batchLag = new HashMap<>();
        batchLag.put(topicPartitions.get(0), 1L);
        batchLag.put(topicPartitions.get(1), 5L);
        batchLag.put(topicPartitions.get(2), 0L);
        batchLag.put(topicPartitions.get(3), 5L);

        PollSelectionContext context = mock(PollSelectionContext.class);
        when(context.currentBatchLag(eq(batchLag.keySet()), anyLong())).thenReturn(batchLag);

        PollStrategy pollStrategy = PollStrategy.greatestBatchLag();
        pollStrategy.onPollingPermitted(topicPartitions);
        pollStrategy.prepareForPoll(context);

        verify(context).selectExclusively(eqSelection(topicPartitions, 1, 3));
    }

    @Test
    public void prepareForPoll_givenPriorityCutoffOnLag_expectsPrioritizedSelectionWithLagCutoff() {
        List<TopicPartition> topicPartitions = Arrays.asList(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1),
                new TopicPartition("topic", 2),
                new TopicPartition("topic", 3));

        Map<TopicPartition, Long> lag = new HashMap<>();
        lag.put(topicPartitions.get(0), 0L);
        lag.put(topicPartitions.get(1), 1L);
        lag.put(topicPartitions.get(2), 2L);
        lag.put(topicPartitions.get(3), 3L);

        PollSelectionContext context = mock(PollSelectionContext.class);
        when(context.currentLag(eq(lag.keySet()), anyLong())).thenReturn(lag);

        Comparator<TopicPartition> priorityComparator = Comparator.comparing(TopicPartition::partition);
        PollStrategy pollStrategy = PollStrategy.priorityCutoffOnLag(priorityComparator, 2);
        pollStrategy.onPollingPermitted(topicPartitions);
        pollStrategy.prepareForPoll(context);

        verify(context).selectExclusively(eqSelection(topicPartitions, 0, 1, 2));
    }

    @Test
    public void onPoll_givenPriorityCutoffOnLag_expectsConsumerRecordsOrderedByPriority() {
        List<TopicPartition> orderedPartitions = Arrays.asList(
                new TopicPartition("topic", 0),
                new TopicPartition("topic", 1),
                new TopicPartition("topic", 2),
                new TopicPartition("topic", 3));

        Map<TopicPartition, List<ConsumerRecord<String, String>>> consumerRecords = new LinkedHashMap<>();
        consumerRecords.put(
                orderedPartitions.get(0),
                Collections.singletonList(createConsumerRecord(orderedPartitions.get(0), 0L, "key", "value")));
        consumerRecords.put(
                orderedPartitions.get(2),
                Collections.singletonList(createConsumerRecord(orderedPartitions.get(2), 0L, "key", "value")));
        consumerRecords.put(
                orderedPartitions.get(3),
                Collections.singletonList(createConsumerRecord(orderedPartitions.get(3), 0L, "key", "value")));
        consumerRecords.put(
                orderedPartitions.get(1),
                Collections.singletonList(createConsumerRecord(orderedPartitions.get(1), 0L, "key", "value")));

        Comparator<TopicPartition> priorityComparator = Comparator.comparing(TopicPartition::partition);
        PollStrategy pollStrategy = PollStrategy.priorityCutoffOnLag(priorityComparator, 2);
        pollStrategy.onPollingPermitted(orderedPartitions);

        ConsumerRecords<String, String> result = pollStrategy.onPoll(new ConsumerRecords<>(consumerRecords));

        // Verify count and order with a single assertion
        assertEquals(orderedPartitions, new ArrayList<>(result.partitions()));
    }

    private <K, V> ConsumerRecord<K, V> createConsumerRecord(TopicPartition partition, long offset, K key, V value) {
        return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, key, value);
    }

    private static <T> Set<T> eqSelection(List<T> list, int... indices) {
        Collection<T> selection = IntStream.of(indices).mapToObj(list::get).collect(Collectors.toSet());
        return argThat(it -> it.size() == selection.size() && it.containsAll(selection));
    }
}
