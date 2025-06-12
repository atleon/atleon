package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.function.Consumer;

/**
 * A wrapper around a Kafka {@link ConsumerRecord} that has been received and is awaiting
 * acknowledgement.
 *
 * @param <K> The type of key contained in this record's ConsumerRecord
 * @param <V> The type of value contained in this record's ConsumerRecord
 */
public final class KafkaReceiverRecord<K, V> {

    private final ConsumerRecord<K, V> consumerRecord;

    private final Runnable acknowledger;

    private final Consumer<Throwable> nacknowledger;

    private KafkaReceiverRecord(
        ConsumerRecord<K, V> consumerRecord,
        Runnable acknowledger,
        Consumer<Throwable> nacknowledger
    ) {
        this.consumerRecord = consumerRecord;
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
    }

    static <K, V> KafkaReceiverRecord<K, V> create(
        ConsumerRecord<K, V> consumerRecord,
        Runnable acknowledger,
        Consumer<Throwable> nacknowledger
    ) {
        return new KafkaReceiverRecord<>(consumerRecord, acknowledger, nacknowledger);
    }

    public TopicPartition topicPartition() {
        return ConsumerRecordExtraction.topicPartition(consumerRecord);
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

    public void nacknowledge(Throwable error) {
        nacknowledger.accept(error);
    }

    public Consumer<Throwable> nacknowledger() {
        return nacknowledger;
    }
}
