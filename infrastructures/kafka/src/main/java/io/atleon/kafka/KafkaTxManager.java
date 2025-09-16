package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * A Kafka-specific manager of transactions.
 */
public interface KafkaTxManager {

    Mono<Void> begin();

    Mono<Void> sendOffsets(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata metadata);

    Mono<Void> commit();

    Mono<Void> abort();
}
