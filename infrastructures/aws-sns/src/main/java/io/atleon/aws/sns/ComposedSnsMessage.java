package io.atleon.aws.sns;

import software.amazon.awssdk.services.sns.model.MessageAttributeValue;

import java.util.Collections;
import java.util.Map;

/**
 * A convenient implementation of {@link SnsMessage} that's composed of and built by all possible
 * properties.
 *
 * @param <T> The (deserialized) type of the body held by this Message
 */
public final class ComposedSnsMessage<T> extends AbstractSnsMessage<T> {

    private ComposedSnsMessage(
            String messageDeduplicationId,
            String messageGroupId,
            Map<String, MessageAttributeValue> messageAttributes,
            String messageStructure,
            String subject,
            T body) {
        super(messageDeduplicationId, messageGroupId, messageAttributes, messageStructure, subject, body);
    }

    public static <T> SnsMessage<T> fromBody(T body) {
        return newBuilder(body).build();
    }

    public static <T> Builder<T> newBuilder(T body) {
        return ComposedSnsMessage.<T>newBuilder().body(body);
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    public static final class Builder<T> {

        private String messageDeduplicationId;

        private String messageGroupId;

        private Map<String, MessageAttributeValue> messageAttributes = Collections.emptyMap();

        private String messageStructure;

        private String subject;

        private T body;

        private Builder() {}

        public ComposedSnsMessage<T> build() {
            return new ComposedSnsMessage<>(
                    messageDeduplicationId, messageGroupId, messageAttributes, messageStructure, subject, body);
        }

        public Builder<T> messageDeduplicationId(String messageDeduplicationId) {
            this.messageDeduplicationId = messageDeduplicationId;
            return this;
        }

        public Builder<T> messageGroupId(String messageGroupId) {
            this.messageGroupId = messageGroupId;
            return this;
        }

        public Builder<T> messageAttributes(Map<String, MessageAttributeValue> messageAttributes) {
            this.messageAttributes = messageAttributes;
            return this;
        }

        public Builder<T> messageStructure(String messageStructure) {
            this.messageStructure = messageStructure;
            return this;
        }

        public Builder<T> subject(String subject) {
            this.subject = subject;
            return this;
        }

        public Builder<T> body(T body) {
            this.body = body;
            return this;
        }
    }
}
