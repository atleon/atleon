package io.atleon.aws.sns;

import software.amazon.awssdk.services.sns.model.MessageAttributeValue;

import java.util.Map;
import java.util.UUID;

/**
 * An {@link SnsMessage} with serialized String body that can be sent to SQS. Each message may
 * reference correlated metadata that is propagated to the {@link SnsSenderResult} that results
 * from the sending of this Message.
 *
 * @param <C> The type of correlated metadata propagated from this Message to its send Result
 */
public final class SnsSenderMessage<C> extends AbstractSnsMessage<String> {
    
    private final String requestId;

    private final C correlationMetadata;

    private SnsSenderMessage(
        String requestId,
        String messageDeduplicationId,
        String messageGroupId,
        Map<String, MessageAttributeValue> messageAttributes,
        String messageStructure,
        String subject,
        String body,
        C correlationMetadata
    ) {
        super(messageDeduplicationId, messageGroupId, messageAttributes, messageStructure, subject, body);
        this.requestId = requestId;
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

    public C correlationMetadata() {
        return correlationMetadata;
    }

    public static final class Builder<C> {

        private final String requestId;

        private String messageDeduplicationId;

        private String messageGroupId;

        private Map<String, MessageAttributeValue> messageAttributes;

        private String messageStructure;

        private String subject;

        private String body;

        private C correlationMetadata;

        private Builder(String requestId) {
            this.requestId = requestId;
        }

        public SnsSenderMessage<C> build() {
            return new SnsSenderMessage<>(
                requestId,
                messageDeduplicationId,
                messageGroupId,
                messageAttributes,
                messageStructure,
                subject,
                body,
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

        public Builder<C> messageStructure(String messageStructure) {
            this.messageStructure = messageStructure;
            return this;
        }

        public Builder<C> subject(String subject) {
            this.subject = subject;
            return this;
        }

        public Builder<C> body(String body) {
            this.body = body;
            return this;
        }

        public Builder<C> correlationMetadata(C correlationMetadata) {
            this.correlationMetadata = correlationMetadata;
            return this;
        }
    }
}
