package io.atleon.kafka;

import io.atleon.core.Alo;
import io.atleon.core.SenderResult;
import java.util.Optional;
import org.apache.kafka.clients.producer.RecordMetadata;

public final class KafkaSenderResult<T> implements SenderResult {

    private final RecordMetadata recordMetadata;

    private final Exception exception;

    private final T correlationMetadata;

    private KafkaSenderResult(RecordMetadata recordMetadata, Exception exception, T correlationMetadata) {
        this.recordMetadata = recordMetadata;
        this.exception = exception;
        this.correlationMetadata = correlationMetadata;
    }

    static <T> KafkaSenderResult<T> success(RecordMetadata recordMetadata, T correlationMetadata) {
        return new KafkaSenderResult<>(recordMetadata, null, correlationMetadata);
    }

    static <T> KafkaSenderResult<T> failure(Exception exception, T correlationMetadata) {
        return new KafkaSenderResult<>(null, exception, correlationMetadata);
    }

    static <T> Alo<KafkaSenderResult<T>> invertAlo(KafkaSenderResult<Alo<T>> senderResult) {
        return senderResult
                .correlationMetadata()
                .map(correlationMetadata -> new KafkaSenderResult<>(
                        senderResult.recordMetadata, senderResult.exception, correlationMetadata));
    }

    static <T> KafkaSenderResult<T> fromSenderResult(reactor.kafka.sender.SenderResult<T> senderResult) {
        return new KafkaSenderResult<>(
                senderResult.recordMetadata(), senderResult.exception(), senderResult.correlationMetadata());
    }

    static <T> Alo<KafkaSenderResult<T>> fromSenderResultOfAlo(reactor.kafka.sender.SenderResult<Alo<T>> senderResult) {
        return senderResult
                .correlationMetadata()
                .map(correlationMetadata -> new KafkaSenderResult<>(
                        senderResult.recordMetadata(), senderResult.exception(), correlationMetadata));
    }

    @Override
    public Optional<Throwable> failureCause() {
        return Optional.ofNullable(exception);
    }

    @Override
    public String toString() {
        return "KafkaSenderResult{" + "recordMetadata="
                + recordMetadata + ", exception="
                + exception + ", correlationMetadata="
                + correlationMetadata + '}';
    }

    public Optional<RecordMetadata> recordMetadata() {
        return Optional.ofNullable(recordMetadata);
    }

    /**
     * Deprecated - use {@link KafkaSenderResult#failureCause()}
     */
    @Deprecated
    public Optional<Exception> exception() {
        return Optional.ofNullable(exception);
    }

    public T correlationMetadata() {
        return correlationMetadata;
    }
}
