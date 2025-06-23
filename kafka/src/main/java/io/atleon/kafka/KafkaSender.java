package io.atleon.kafka;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * A reactive sender of Kafka {@link org.apache.kafka.clients.producer.ProducerRecord records},
 * wrapped as {@link KafkaSenderRecord}.
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
            .cacheInvalidateWhen(__ -> closeSink.asFlux().next().then(), SendingProducer::close);
    }

    public static <K, V> KafkaSender<K, V> create(KafkaSenderOptions<K, V> options) {
        return new KafkaSender<>(options);
    }

    public <T> Mono<KafkaSenderResult<T>> send(KafkaSenderRecord<K, V, T> senderRecord) {
        return Mono.just(senderRecord).as(this::send).single();
    }

    public <T> Flux<KafkaSenderResult<T>> send(Publisher<KafkaSenderRecord<K, V, T>> senderRecords) {
        return futureProducer.flatMapMany(producer -> SendOperator.immediate(options, producer, senderRecords));
    }

    public <T> Flux<KafkaSenderResult<T>> sendDelayError(Publisher<KafkaSenderRecord<K, V, T>> senderRecords) {
        return futureProducer.flatMapMany(producer -> SendOperator.delayError(options, producer, senderRecords));
    }

    public <T> Flux<KafkaSenderResult<T>> sendDelegateError(Publisher<KafkaSenderRecord<K, V, T>> senderRecords) {
        return futureProducer.flatMapMany(producer -> SendOperator.delegateError(options, producer, senderRecords));
    }

    public void close() {
        closeSink.tryEmitNext(System.currentTimeMillis());
    }
}
