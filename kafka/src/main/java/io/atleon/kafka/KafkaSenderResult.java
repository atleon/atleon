package io.atleon.kafka;

import io.atleon.core.Alo;
import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.kafka.sender.SenderResult;

import java.util.Optional;

public class KafkaSenderResult<T> {

    private final RecordMetadata recordMetadata;

    private final Exception exception;

    private final T correlationMetadata;

    private KafkaSenderResult(RecordMetadata recordMetadata, Exception exception, T correlationMetadata) {
        this.recordMetadata = recordMetadata;
        this.exception = exception;
        this.correlationMetadata = correlationMetadata;
    }

    static <T> KafkaSenderResult<T> fromSenderResult(SenderResult<T> senderResult) {
        return new KafkaSenderResult<>(senderResult.recordMetadata(), senderResult.exception(), senderResult.correlationMetadata());
    }

    static <T> Alo<KafkaSenderResult<T>> fromSenderResultOfAlo(SenderResult<Alo<T>> senderResult) {
        return senderResult.correlationMetadata().map(correlationMetadata ->
            new KafkaSenderResult<>(senderResult.recordMetadata(), senderResult.exception(), correlationMetadata));
    }

    @Override
    public String toString() {
        return "KafkaSenderResult{" +
            "recordMetadata=" + recordMetadata +
            ", exception=" + exception +
            ", correlationMetadata=" + correlationMetadata +
            '}';
    }

    public Optional<RecordMetadata> recordMetadata() {
        return Optional.ofNullable(recordMetadata);
    }

    public Optional<Exception> exception() {
        return Optional.ofNullable(exception);
    }

    public T correlationMetadata() {
        return correlationMetadata;
    }
}
