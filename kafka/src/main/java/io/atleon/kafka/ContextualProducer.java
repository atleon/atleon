package io.atleon.kafka;

import io.atleon.core.Contextual;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Kafka {@link Producer} that is aware of the fact that all sent {@link ProducerRecord}s are
 * actually {@link SenderRecord}s. Sending is then contextualized when the SenderRecord's
 * correlation metadata is {@link Contextual}.
 *
 * @param <K> The type of keys produced
 * @param <V> The type of values produced
 */
final class ContextualProducer<K, V> implements Producer<K, V> {

    private final Producer<K, V> delegate;

    public ContextualProducer(Producer<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void initTransactions() {
        delegate.initTransactions();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        delegate.beginTransaction();
    }

    @Override
    public void sendOffsetsToTransaction(
        Map<TopicPartition, OffsetAndMetadata> offsets,
        String consumerGroupId
    ) throws ProducerFencedException {
        delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
    }

    @Override
    public void sendOffsetsToTransaction(
        Map<TopicPartition, OffsetAndMetadata> offsets,
        ConsumerGroupMetadata groupMetadata
    ) throws ProducerFencedException {
        delegate.sendOffsetsToTransaction(offsets, groupMetadata);
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        delegate.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        delegate.abortTransaction();
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        Object correlationMetadata = record instanceof SenderRecord
            ? SenderRecord.class.cast(record).correlationMetadata()
            : KafkaSenderRecord.class.cast(record).correlationMetadata();
        return correlationMetadata instanceof Contextual
            ? Contextual.class.cast(correlationMetadata).supplyInContext(() -> delegate.send(record))
            : delegate.send(record);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        Object correlationMetadata = record instanceof SenderRecord
            ? SenderRecord.class.cast(record).correlationMetadata()
            : KafkaSenderRecord.class.cast(record).correlationMetadata();
        return correlationMetadata instanceof Contextual
            ? Contextual.class.cast(correlationMetadata).supplyInContext(() -> delegate.send(record, callback))
            : delegate.send(record, callback);
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return delegate.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return delegate.metrics();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void close(Duration timeout) {
        delegate.close(timeout);
    }
}
