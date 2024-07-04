package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test extension of {@link ListConsumerGroupOffsetsResult} to allow creation for tests
 */
public class TestListConsumerGroupOffsetsResult extends ListConsumerGroupOffsetsResult{

    private TestListConsumerGroupOffsetsResult(
        Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures
    ) {
        super(futures);
    }

    public static TestListConsumerGroupOffsetsResult create(
        Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures
    ) {
        Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> convertedFutures =
            futures.entrySet()
                .stream()
                .collect(Collectors.toMap(it -> CoordinatorKey.byGroupId(it.getKey()), Map.Entry::getValue));
        return new TestListConsumerGroupOffsetsResult(convertedFutures);
    }
}
