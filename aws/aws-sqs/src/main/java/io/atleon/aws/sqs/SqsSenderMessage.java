package io.atleon.aws.sqs;

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * An {@link SqsMessage} with serialized String body that can be sent to SQS. Each message may
 * reference correlated metadata that is propagated to the {@link SqsSenderResult} that results
 * from the sending of this Message.
 *
 * @param <C> The type of correlated metadata propagated from this Message to its send Result
 */
public final class SqsSenderMessage<C> extends AbstractSqsMessage<String> {

    private final String requestId;

    private final String messageDeduplicationId;

    private final String messageGroupId;

    private final Integer delaySeconds;

    private final C correlationMetadata;

    private SqsSenderMessage(
        String requestId,
        String messageDeduplicationId,
        String messageGroupId,
        Map<String, MessageAttributeValue> messageAttributes,
        Map<String, MessageSystemAttributeValue> messageSystemAttributes,
        String body,
        Integer delaySeconds,
        C correlationMetadata
    ) {
        super(messageAttributes, messageSystemAttributes, body);
        this.requestId = requestId;
        this.messageDeduplicationId = messageDeduplicationId;
        this.messageGroupId = messageGroupId;
        this.delaySeconds = delaySeconds;
        this.correlationMetadata = correlationMetadata;
    }

    public static <C> Builder<C> newBuilder() {
        return new Builder<>(UUID.randomUUID().toString());
    }

    /**
     * An internally-used identifier for Send Requests made for this Message.
     */
    public String requestId() {
        return requestId;
    }

    @Override
    public Optional<String> messageDeduplicationId() {
        return Optional.ofNullable(messageDeduplicationId);
    }

    @Override
    public Optional<String> messageGroupId() {
        return Optional.ofNullable(messageGroupId);
    }

    @Override
    public Optional<Integer> senderDelaySeconds() {
        return Optional.ofNullable(delaySeconds);
    }

    /**
     * The correlated metadata propagated from this Message to its Send Result.
     */
    public C correlationMetadata() {
        return correlationMetadata;
    }

    public static final class Builder<C> {

        private final String requestId;

        private String messageDeduplicationId;

        private String messageGroupId;

        private Map<String, MessageAttributeValue> messageAttributes;

        private Map<String, MessageSystemAttributeValue> messageSystemAttributes;

        private String body;

        private Integer delaySeconds;

        private C correlationMetadata;

        private Builder(String requestId) {
            this.requestId = requestId;
        }

        public SqsSenderMessage<C> build() {
            return new SqsSenderMessage<>(
                requestId,
                messageDeduplicationId,
                messageGroupId,
                messageAttributes,
                messageSystemAttributes,
                body,
                delaySeconds,
                correlationMetadata
            );
        }

        public Builder<C> messageDeduplicationId(String messageDeduplicationId) {
            this.messageDeduplicationId = messageDeduplicationId;
            return this;
        }

        public Builder<C> messageGroupId(String messageGroupId) {
            this.messageGroupId = messageGroupId;
            return this;
        }

        public Builder<C> messageAttributes(Map<String, MessageAttributeValue> messageAttributes) {
            this.messageAttributes = messageAttributes;
            return this;
        }

        public Builder<C> messageSystemAttributes(Map<String, MessageSystemAttributeValue> messageSystemAttributes) {
            this.messageSystemAttributes = messageSystemAttributes;
            return this;
        }

        public Builder<C> body(String body) {
            this.body = body;
            return this;
        }

        public Builder<C> delaySeconds(Integer delaySeconds) {
            this.delaySeconds = delaySeconds;
            return this;
        }

        public Builder<C> correlationMetadata(C correlationMetadata) {
            this.correlationMetadata = correlationMetadata;
            return this;
        }
    }
}
