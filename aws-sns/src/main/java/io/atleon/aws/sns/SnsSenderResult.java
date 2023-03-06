package io.atleon.aws.sns;

import io.atleon.core.SenderResult;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * The result of sending an {@link SnsSenderMessage}. Will either contain {@link SuccessMetadata}
 * or a {@link Throwable} error. Will contain the ID of the originating request and that request's
 * correlated metadata.
 *
 * @param <C> The type of correlated metadata that is propagated from the originating request
 */
public final class SnsSenderResult<C> implements SenderResult {

    private final String requestId;

    private final SuccessMetadata successMetadata;

    private final Throwable error;

    private final C correlationMetadata;

    private SnsSenderResult(
        String requestId,
        SuccessMetadata successMetadata,
        Throwable error,
        C correlationMetadata
    ) {
        this.requestId = requestId;
        this.successMetadata = successMetadata;
        this.error = error;
        this.correlationMetadata = correlationMetadata;
    }

    public static <C> SnsSenderResult<C> success(
        String requestId,
        String messageId,
        String sequenceNumber,
        C correlationMetadata
    ) {
        SuccessMetadata successMetadata = new SuccessMetadata(messageId, sequenceNumber);
        return new SnsSenderResult<>(requestId, successMetadata, null, correlationMetadata);
    }

    public static <C> SnsSenderResult<C> failure(String requestId, Throwable error, C correlationMetadata) {
        return new SnsSenderResult<>(requestId, null, Objects.requireNonNull(error), correlationMetadata);
    }

    @Override
    public Optional<Throwable> failureCause() {
        return Optional.ofNullable(error);
    }

    public <R> SnsSenderResult<R> replaceCorrelationMetadata(R newCorrelationMetadata) {
        return new SnsSenderResult<>(requestId, successMetadata, error, newCorrelationMetadata);
    }

    public <R> SnsSenderResult<R> mapCorrelationMetadata(Function<? super C, ? extends R> mapper) {
        return new SnsSenderResult<>(requestId, successMetadata, error, mapper.apply(correlationMetadata));
    }

    public String requestId() {
        return requestId;
    }

    public Optional<SuccessMetadata> successMetadata() {
        return Optional.ofNullable(successMetadata);
    }

    public C correlationMetadata() {
        return correlationMetadata;
    }

    /**
     * Upon successfully sending a Message to SNS, this metadata describes the assigned Message ID
     * and, if sent to a FIFO queue, the assigned sequence number.
     */
    public static final class SuccessMetadata {

        private final String messageId;

        private final String sequenceNumber;

        private SuccessMetadata(String messageId, String sequenceNumber) {
            this.messageId = Objects.requireNonNull(messageId);
            this.sequenceNumber = sequenceNumber;
        }

        public String messageId() {
            return messageId;
        }

        public Optional<String> sequenceNumber() {
            return Optional.ofNullable(sequenceNumber);
        }
    }
}
