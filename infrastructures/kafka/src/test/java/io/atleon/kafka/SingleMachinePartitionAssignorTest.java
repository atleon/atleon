package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleMachinePartitionAssignorTest {

    private final SingleMachinePartitionAssignor assignor = new SingleMachinePartitionAssignor();

    @Test
    public void assign_givenMultipleMachines_expectsAllPartitionsAssignedToOneConsumer() {
        String topic = "topic";
        int numPartitions = 4;
        Map<String, Integer> partitionCounts = Collections.singletonMap(topic, numPartitions);

        MachineData oldMachine = new MachineData(UUID.randomUUID(), 12345L);
        MachineData youngMachine = new MachineData(UUID.randomUUID(), 56789L);

        ConsumerPartitionAssignor.Subscription oldSubscription = new ConsumerPartitionAssignor.Subscription(
                Collections.singletonList(topic), oldMachine.toByteBuffer());
        ConsumerPartitionAssignor.Subscription youngSubscription = new ConsumerPartitionAssignor.Subscription(
                Collections.singletonList(topic), youngMachine.toByteBuffer());

        Map<String, ConsumerPartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put("old-member", oldSubscription);
        subscriptionsByMemberId.put("young-member", youngSubscription);

        Map<String, List<TopicPartition>> result = assignor.assign(partitionCounts, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(4, result.get("old-member").size());
        assertTrue(result.get("young-member").isEmpty());
        IntStream.range(0, numPartitions).forEach(it ->
                assertTrue(result.get("old-member").contains(new TopicPartition(topic, it))));
    }

    @Test
    public void assign_givenSingleMachineWithMultipleConsumers_expectsPartitionsBalancedToConsumers() {
        String topic = "topic";
        int numPartitions = 4;
        Map<String, Integer> partitionCounts = Collections.singletonMap(topic, numPartitions);

        MachineData machine = new MachineData(UUID.randomUUID(), 12345L);

        ConsumerPartitionAssignor.Subscription subscription1 = new ConsumerPartitionAssignor.Subscription(
                Collections.singletonList(topic), machine.toByteBuffer());
        ConsumerPartitionAssignor.Subscription subscription2 = new ConsumerPartitionAssignor.Subscription(
                Collections.singletonList(topic), machine.toByteBuffer());

        Map<String, ConsumerPartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put("member-1", subscription1);
        subscriptionsByMemberId.put("member-2", subscription2);

        Map<String, List<TopicPartition>> result = assignor.assign(partitionCounts, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(2, result.get("member-1").size());
        assertEquals(2, result.get("member-2").size());
        assertTrue(result.get("member-1").contains(new TopicPartition(topic, 0)));
        assertTrue(result.get("member-2").contains(new TopicPartition(topic, 1)));
        assertTrue(result.get("member-1").contains(new TopicPartition(topic, 2)));
        assertTrue(result.get("member-2").contains(new TopicPartition(topic, 3)));
    }

    @Test
    public void assign_givenMultipleMachinesWithNonEqualSubscriptions_expectsMutuallyExclusiveAssignments() {
        String topic1 = "topic-1";
        String topic2 = "topic-2";
        int numPartitions = 4;
        Map<String, Integer> partitionCounts = new HashMap<>();
        partitionCounts.put(topic1, numPartitions);
        partitionCounts.put(topic2, numPartitions);

        MachineData oldMachine = new MachineData(UUID.randomUUID(), 12345L);
        MachineData youngMachine = new MachineData(UUID.randomUUID(), 56789L);

        ConsumerPartitionAssignor.Subscription oldSubscription = new ConsumerPartitionAssignor.Subscription(
                Collections.singletonList(topic1), oldMachine.toByteBuffer());
        ConsumerPartitionAssignor.Subscription youngSubscription = new ConsumerPartitionAssignor.Subscription(
                Arrays.asList(topic1, topic2), youngMachine.toByteBuffer());

        Map<String, ConsumerPartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put("old-member", oldSubscription);
        subscriptionsByMemberId.put("young-member", youngSubscription);

        Map<String, List<TopicPartition>> result = assignor.assign(partitionCounts, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(4, result.get("old-member").size());
        assertEquals(4, result.get("young-member").size());
        IntStream.range(0, numPartitions).forEach(it ->
                assertTrue(result.get("old-member").contains(new TopicPartition(topic1, it))));
        IntStream.range(0, numPartitions).forEach(it ->
                assertTrue(result.get("young-member").contains(new TopicPartition(topic2, it))));
    }
}