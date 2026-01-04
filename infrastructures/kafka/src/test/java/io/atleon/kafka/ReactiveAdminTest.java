package io.atleon.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TestDescribeTopicsResult;
import org.apache.kafka.clients.admin.TestListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

class ReactiveAdminTest {

    @Test
    public void listTopicPartitions_givenExistingPartitionsForTopic_expectsReturnedTopicPartitions() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        TopicPartitionInfo partitionInfo = new TopicPartitionInfo(
                topicPartition.partition(), null, Collections.emptyList(), Collections.emptyList());
        TopicDescription topicDescription =
                new TopicDescription(topicPartition.topic(), false, Collections.singletonList(partitionInfo));
        Map<String, KafkaFuture<TopicDescription>> topicDescriptionNameFutures =
                Collections.singletonMap(topicPartition.topic(), KafkaFuture.completedFuture(topicDescription));

        Admin admin = mock(Admin.class);
        when(admin.describeTopics(collectionContaining(topicPartition.topic())))
                .thenReturn(new TestDescribeTopicsResult(topicDescriptionNameFutures));

        List<TopicPartition> results = new ReactiveAdmin(admin)
                .listTopicPartitions(topicPartition.topic())
                .collectList()
                .block();

        assertEquals(Collections.singletonList(topicPartition), results);
    }

    @Test
    public void listOffsets_givenExistingTopicPartitions_expectsReturnedOffsets() {
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        ListOffsetsResult.ListOffsetsResultInfo listOffsetsResult =
                new ListOffsetsResult.ListOffsetsResultInfo(123456L, System.currentTimeMillis(), Optional.empty());
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> listOffsetsResultFutures =
                Collections.singletonMap(topicPartition, KafkaFuture.completedFuture(listOffsetsResult));
        OffsetSpec offsetSpec = OffsetSpec.latest();

        Admin admin = mock(Admin.class);
        when(admin.listOffsets(mapContaining(topicPartition, offsetSpec), any()))
                .thenReturn(new ListOffsetsResult(listOffsetsResultFutures));

        Map<TopicPartition, Long> results = new ReactiveAdmin(admin)
                .listOffsets(Collections.singletonList(topicPartition), offsetSpec)
                .block();

        assertEquals(Collections.singletonMap(topicPartition, listOffsetsResult.offset()), results);
    }

    @Test
    public void listTopicPartitionGroupOffsets_givenCommittedOffsetsForGroup_expectsReturnedOffsets() {
        String groupId = UUID.randomUUID().toString();
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        ListOffsetsResult.ListOffsetsResultInfo listOffsetsResult =
                new ListOffsetsResult.ListOffsetsResultInfo(123456L, System.currentTimeMillis(), Optional.empty());
        Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata =
                Collections.singletonMap(topicPartition, new OffsetAndMetadata(listOffsetsResult.offset(), "metadata"));

        Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> offsetAndMetadataFutures =
                Collections.singletonMap(groupId, KafkaFuture.completedFuture(offsetsAndMetadata));
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> listOffsetsResultFutures =
                Collections.singletonMap(topicPartition, KafkaFuture.completedFuture(listOffsetsResult));

        Admin admin = mock(Admin.class);
        when(admin.listConsumerGroupOffsets(mapContainingKey(groupId)))
                .thenReturn(TestListConsumerGroupOffsetsResult.create(offsetAndMetadataFutures));
        when(admin.listOffsets(mapContainingKey(topicPartition), any()))
                .thenReturn(new ListOffsetsResult(listOffsetsResultFutures));

        List<TopicPartitionGroupOffsets> results = new ReactiveAdmin(admin)
                .listTopicPartitionGroupOffsets(groupId)
                .collectList()
                .block();

        assertEquals(1, results.size());
        assertEquals(0, results.get(0).estimateLag());
        assertEquals(topicPartition, results.get(0).topicPartition());
        assertEquals(groupId, results.get(0).groupId());
    }

    @Test
    public void
            listTopicPartitionGroupOffsets_givenEarliestResetForGroup_expectsReturnedOffsetsForPartitionsWithData() {
        String groupId = UUID.randomUUID().toString();
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        ListOffsetsResult.ListOffsetsResultInfo earliestOffsetsResult =
                new ListOffsetsResult.ListOffsetsResultInfo(0L, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo latestOffsetsResult =
                new ListOffsetsResult.ListOffsetsResultInfo(123456L, System.currentTimeMillis(), Optional.empty());

        Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> offsetAndMetadataFutures =
                Collections.singletonMap(groupId, KafkaFuture.completedFuture(Collections.emptyMap()));
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsetsResultFutures =
                Collections.singletonMap(topicPartition, KafkaFuture.completedFuture(earliestOffsetsResult));
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> latestOffsetsResultFutures =
                Collections.singletonMap(topicPartition, KafkaFuture.completedFuture(latestOffsetsResult));

        Admin admin = mock(Admin.class);
        when(admin.listConsumerGroupOffsets(mapContainingKey(groupId)))
                .thenReturn(TestListConsumerGroupOffsetsResult.create(offsetAndMetadataFutures));
        when(admin.listOffsets(mapContaining(topicPartition, OffsetSpec.EarliestSpec.class::isInstance), any()))
                .thenReturn(new ListOffsetsResult(earliestOffsetsResultFutures));
        when(admin.listOffsets(mapContaining(topicPartition, OffsetSpec.LatestSpec.class::isInstance), any()))
                .thenReturn(new ListOffsetsResult(latestOffsetsResultFutures));

        Map<String, OffsetResetStrategy> groupIds = Collections.singletonMap(groupId, OffsetResetStrategy.EARLIEST);
        List<TopicPartitionGroupOffsets> results = new ReactiveAdmin(admin)
                .listTopicPartitionGroupOffsets(groupIds, Collections.singletonList(topicPartition))
                .collectList()
                .block();

        assertEquals(1, results.size());
        assertEquals(
                latestOffsetsResult.offset() - earliestOffsetsResult.offset(),
                results.get(0).estimateLag());
        assertEquals(topicPartition, results.get(0).topicPartition());
        assertEquals(groupId, results.get(0).groupId());
        assertEquals(0, results.get(0).groupOffset());
    }

    @Test
    public void listTopicPartitionGroupOffsets_givenNoResetForGroup_expectsNoReturnedOffsetsForPartitions() {
        String groupId = UUID.randomUUID().toString();
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        ListOffsetsResult.ListOffsetsResultInfo earliestOffsetsResult =
                new ListOffsetsResult.ListOffsetsResultInfo(0L, System.currentTimeMillis(), Optional.empty());
        ListOffsetsResult.ListOffsetsResultInfo latestOffsetsResult =
                new ListOffsetsResult.ListOffsetsResultInfo(123456L, System.currentTimeMillis(), Optional.empty());

        Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> offsetAndMetadataFutures =
                Collections.singletonMap(groupId, KafkaFuture.completedFuture(Collections.emptyMap()));
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> earliestOffsetsResultFutures =
                Collections.singletonMap(topicPartition, KafkaFuture.completedFuture(earliestOffsetsResult));
        Map<TopicPartition, KafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> latestOffsetsResultFutures =
                Collections.singletonMap(topicPartition, KafkaFuture.completedFuture(latestOffsetsResult));

        Admin admin = mock(Admin.class);
        when(admin.listConsumerGroupOffsets(mapContainingKey(groupId)))
                .thenReturn(TestListConsumerGroupOffsetsResult.create(offsetAndMetadataFutures));
        when(admin.listOffsets(mapContaining(topicPartition, OffsetSpec.EarliestSpec.class::isInstance), any()))
                .thenReturn(new ListOffsetsResult(earliestOffsetsResultFutures));
        when(admin.listOffsets(mapContaining(topicPartition, OffsetSpec.LatestSpec.class::isInstance), any()))
                .thenReturn(new ListOffsetsResult(latestOffsetsResultFutures));

        Map<String, OffsetResetStrategy> groupIds = Collections.singletonMap(groupId, OffsetResetStrategy.NONE);
        List<TopicPartitionGroupOffsets> results = new ReactiveAdmin(admin)
                .listTopicPartitionGroupOffsets(groupIds, Collections.singletonList(topicPartition))
                .collectList()
                .block();

        assertTrue(results.isEmpty());
    }

    private static <T> Collection<T> collectionContaining(T t) {
        return argThat(it -> it != null && it.contains(t));
    }

    private static <K, V> Map<K, V> mapContainingKey(K key) {
        return argThat(it -> it != null && it.containsKey(key));
    }

    private static <K, V> Map<K, V> mapContaining(K key, V value) {
        return argThat(it -> it != null && it.containsKey(key) && Objects.equals(value, it.get(key)));
    }

    private static <K, V> Map<K, V> mapContaining(K key, Predicate<V> valuePredicate) {
        return argThat(it -> it != null && it.containsKey(key) && valuePredicate.test(it.get(key)));
    }
}
