package io.atleon.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

/**
 * An extension of {@link ProducerRecord} that can be sent reactively.
 *
 * @param <K> The type of keys in this record
 * @param <V> The type of values in this record
 */
public final class KafkaSenderRecord<K, V, T> extends ProducerRecord<K, V> {

    private final T correlationMetadata;

    private KafkaSenderRecord(
        String topic,
        Integer partition,
        Long timestamp,
        K key,
        V value,
        Iterable<Header> headers,
        T correlationMetadata
    ) {
        super(topic, partition, timestamp, key, value, headers);
        this.correlationMetadata = correlationMetadata;
    }

    public static <K, V, T> KafkaSenderRecord<K, V, T> create(
        ProducerRecord<K, V> producerRecord,
        T correlationMetadata
    ) {
        return new KafkaSenderRecord<>(
            producerRecord.topic(),
            producerRecord.partition(),
            producerRecord.timestamp(),
            producerRecord.key(),
            producerRecord.value(),
            producerRecord.headers(),
            correlationMetadata
        );
    }

    public static <K, V, T> KafkaSenderRecord<K, V, T> create(String topic, K key, V value, T correlationMetadata) {
        return new KafkaSenderRecord<>(topic, null, null, key, value, null, correlationMetadata);
    }

    public static <K, V, T> KafkaSenderRecord<K, V, T> create(
        String topic,
        K key,
        V value,
        Iterable<Header> headers,
        T correlationMetadata
    ) {
        return new KafkaSenderRecord<>(topic, null, null, key, value, headers, correlationMetadata);
    }

    public static <K, V, T> KafkaSenderRecord<K, V, T> create(
        String topic,
        Integer partition,
        K key,
        V value,
        T correlationMetadata
    ) {
        return new KafkaSenderRecord<>(topic, partition, null, key, value, null, correlationMetadata);
    }

    public static <K, V, T> KafkaSenderRecord<K, V, T> create(
        String topic,
        Integer partition,
        K key,
        V value,
        Iterable<Header> headers,
        T correlationMetadata
    ) {
        return new KafkaSenderRecord<>(topic, partition, null, key, value, headers, correlationMetadata);
    }

    public T correlationMetadata() {
        return correlationMetadata;
    }
}
