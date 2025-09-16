package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Optional;

/**
 * A received {@link ConsumerRecord} that is eligible for downstream emission, but has not been
 * emitted yet.
 */
final class EmittableRecord<K, V> {

    private final ActivePartition activePartition;

    private final ConsumerRecord<K, V> consumerRecord;

    public EmittableRecord(ActivePartition activePartition, ConsumerRecord<K, V> consumerRecord) {
        this.activePartition = activePartition;
        this.consumerRecord = consumerRecord;
    }

    public Optional<KafkaReceiverRecord<K, V>> activateForProcessing() {
        return activePartition.activateForProcessing(consumerRecord);
    }

    public TopicPartition topicPartition() {
        return activePartition.topicPartition();
    }
}
