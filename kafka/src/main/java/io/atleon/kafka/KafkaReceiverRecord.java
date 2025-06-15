package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * A wrapper around a Kafka {@link ConsumerRecord} that has been received and is awaiting
 * acknowledgement.
 *
 * @param <K> The type of key contained in this record's ConsumerRecord
 * @param <V> The type of value contained in this record's ConsumerRecord
 */
public final class KafkaReceiverRecord<K, V> {

    private final TopicPartition topicPartition;

    private final ConsumerRecord<K, V> consumerRecord;

    private final Runnable acknowledger;

    private KafkaReceiverRecord(
        TopicPartition topicPartition,
        ConsumerRecord<K, V> consumerRecord,
        Runnable acknowledger
    ) {
        this.topicPartition = topicPartition;
        this.consumerRecord = consumerRecord;
        this.acknowledger = acknowledger;
    }

    static <K, V> KafkaReceiverRecord<K, V> create(
        TopicPartition topicPartition,
        ConsumerRecord<K, V> consumerRecord,
        Runnable acknowledger
    ) {
        return new KafkaReceiverRecord<>(topicPartition, consumerRecord, acknowledger);
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public ConsumerRecord<K, V> consumerRecord() {
        return consumerRecord;
    }

    public void acknowledge() {
        acknowledger.run();
    }

    public Runnable acknowledger() {
        return acknowledger;
    }
}
