package io.atleon.kafka;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
            new TopicPartition("topic", 3)
        );

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

    private static <T> Set<T> eqSelection(List<T> list, int... indices) {
        Collection<T> selection = IntStream.of(indices).mapToObj(list::get).collect(Collectors.toSet());
        return argThat(it -> it.size() == selection.size() && it.containsAll(selection));
    }
}