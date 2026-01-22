package io.atleon.kafka;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Map;

/**
 * A reactive sender for Kafka {@link org.apache.kafka.clients.producer.ProducerRecord records},
 * wrapped as {@link KafkaSenderRecord}. This class provides a high-level API for producing records
 * to Kafka topics in a non-blocking, asynchronous, and backpressure-aware manner using Reactor.
 * <p>
 * Each instance of {@code KafkaSender} manages its own Kafka
 * {@link org.apache.kafka.clients.producer.Producer}. Records can be sent with or without custom
 * correlation metadata, and results are emitted as {@link KafkaSenderResult} objects, which
 * provide access to Kafka metadata and any error that occurred during sending.
 * <p>
 * This sender supports transactional sending when configured appropriately (setting
 * {@link org.apache.kafka.clients.producer.ProducerConfig#TRANSACTIONAL_ID_CONFIG}), allowing
 * for atomic writes of multiple records (and offset commits if/when transaction manager is also
 * used for reception).
 *
 * @param <K> The type of keys in records produced by this sender
 * @param <V> The type of values in records produced by this sender
 */
public final class KafkaSender<K, V> {

    private final KafkaSenderOptions<K, V> options;

    private final Mono<SendingProducer<K, V>> futureProducer;

    private final Sinks.Many<Long> closeSink = Sinks.many().multicast().directBestEffort();

    private KafkaSender(KafkaSenderOptions<K, V> options) {
        this.options = options;
        this.futureProducer = Mono.fromSupplier(() -> new SendingProducer<>(options))
                .cacheInvalidateWhen(
                        it -> closeSink.asFlux().next().then().or(it.closed()), SendingProducer::closeSafelyAsync);
    }

    public static <K, V> KafkaSender<K, V> create(KafkaSenderOptions<K, V> options) {
        return new KafkaSender<>(options);
    }

    /**
     * Sends a single {@link KafkaSenderRecord} to Kafka. The resulting Mono will either emit an
     * error if the send operation fails, or a {@link KafkaSenderResult} if the send operation
     * succeeds.
     *
     * @param senderRecord The record to send
     * @param <T>          The type of correlation metadata
     * @return a {@link Mono} emitting the result of the send operation
     */
    public <T> Mono<KafkaSenderResult<T>> send(KafkaSenderRecord<K, V, T> senderRecord) {
        return Mono.just(senderRecord).as(this::send).single();
    }

    /**
     * Sends a stream of {@link KafkaSenderRecord}s to Kafka and emits results for each record as
     * they are returned from the underlying producer. Errors are propagated immediately and
     * terminate the stream.
     *
     * @param senderRecords The publisher of records to send
     * @param <T>           The type of correlation metadata
     * @return a {@link Flux} emitting (successful) results for each sent record
     */
    public <T> Flux<KafkaSenderResult<T>> send(Publisher<KafkaSenderRecord<K, V, T>> senderRecords) {
        return futureProducer.flatMapMany(producer -> SendPublisher.immediateError(options, producer, senderRecords));
    }

    /**
     * Sends a stream of {@link KafkaSenderRecord}s to Kafka and emits results for each record as
     * they are returned from the underlying producer. Errors are delegated to the client for
     * handling by encapsulating both send failures and successes in emitted results.
     *
     * @param senderRecords The publisher of records to send
     * @param <T>           The type of correlation metadata
     * @return a {@link Flux} emitting results for each sent record, with errors delegated per record
     */
    public <T> Flux<KafkaSenderResult<T>> sendDelegateError(Publisher<KafkaSenderRecord<K, V, T>> senderRecords) {
        return futureProducer.flatMapMany(producer -> SendPublisher.delegateError(options, producer, senderRecords));
    }

    /**
     * Sends a stream of {@link KafkaSenderRecord}s to Kafka and emits results for each record as
     * they are returned from the producer. Errors are delayed until all records have been
     * processed, after which the first-occurring send error is propagated as an error signal.
     *
     * @param senderRecords The publisher of records to send
     * @param <T>           The type of correlation metadata
     * @return a {@link Flux} emitting results for each sent record
     */
    public <T> Flux<KafkaSenderResult<T>> sendDelayError(Publisher<KafkaSenderRecord<K, V, T>> senderRecords) {
        return futureProducer.flatMapMany(producer -> SendPublisher.delayError(options, producer, senderRecords));
    }

    /**
     * Sends batches of records to Kafka in separate transactions, ensuring atomicity per batch.
     * Each inner {@link Publisher} represents a batch of records sent in a single transaction
     * session.
     *
     * @param batches A publisher of record batches, each sent in separate transaction sessions
     * @param <T>     The type of correlation metadata
     * @return a {@link Flux} emitting results for each sent record
     */
    public <T> Flux<KafkaSenderResult<T>> sendTransactional(
            Publisher<? extends Publisher<KafkaSenderRecord<K, V, T>>> batches) {
        return futureProducer.delayUntil(SendingProducer::initTransactions).flatMapMany(producer -> Flux.from(batches)
                .concatMap(batch -> sendTransactional(producer, batch)));
    }

    /**
     * Provides a transaction manager that facilitates transaction operations on the underlying
     * Kafka producer. This manager can be used to begin, commit, and abort transactions, ensuring
     * atomicity and consistency when producing records to Kafka. When combined with transactional
     * reception from Kafka, this can be used to accomplish exactly-once processing semantics.
     *
     * @return a {@link Mono} that will emit an initialized transaction manager
     */
    public Mono<KafkaTxManager> txManager() {
        return futureProducer.delayUntil(SendingProducer::initTransactions).map(ProducerTxManager::new);
    }

    public void close() {
        closeSink.tryEmitNext(System.currentTimeMillis());
    }

    private <T> Flux<KafkaSenderResult<T>> sendTransactional(
            SendingProducer<K, V> producer, Publisher<KafkaSenderRecord<K, V, T>> senderRecords) {
        return Flux.usingWhen(
                producer.beginTransaction().thenReturn(producer),
                transactionalProducer -> SendPublisher.immediateError(options, transactionalProducer, senderRecords),
                SendingProducer::commitTransaction,
                (transactionalProducer, error) -> transactionalProducer.abortTransaction(),
                SendingProducer::abortTransaction);
    }

    private static final class ProducerTxManager implements KafkaTxManager {

        private final SendingProducer<?, ?> producer;

        public ProducerTxManager(SendingProducer<?, ?> producer) {
            this.producer = producer;
        }

        @Override
        public Mono<Void> begin() {
            return producer.beginTransaction();
        }

        @Override
        public Mono<Void> sendOffsets(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata metadata) {
            return producer.sendOffsetsToTransaction(offsets, metadata);
        }

        @Override
        public Mono<Void> commit() {
            return producer.commitTransaction();
        }

        @Override
        public Mono<Void> abort() {
            return producer.abortTransaction();
        }
    }
}
