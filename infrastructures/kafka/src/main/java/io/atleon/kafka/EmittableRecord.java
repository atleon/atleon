package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

import java.util.function.Consumer;

/**
 * A received {@link ConsumerRecord} that is eligible for downstream emission, but has not been
 * emitted yet.
 */
final class EmittableRecord<K, V> {

    private final ActivePartition<K, V> activePartition;

    private final ConsumerRecord<K, V> consumerRecord;

    public EmittableRecord(ActivePartition<K, V> activePartition, ConsumerRecord<K, V> consumerRecord) {
        this.activePartition = activePartition;
        this.consumerRecord = consumerRecord;
    }

    public @Nullable KafkaReceiverRecord<K, V> activateForProcessing(Consumer<TopicPartition> onActivate) {
        return activePartition.activateForProcessing(consumerRecord, onActivate).orElse(null);
    }
}
