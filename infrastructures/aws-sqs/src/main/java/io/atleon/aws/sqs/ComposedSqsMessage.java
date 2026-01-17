package io.atleon.aws.sqs;

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * A convenient implementation of {@link SqsMessage} that's composed of and built by all possible
 * properties.
 *
 * @param <T> The (deserialized) type of the body held by this Message
 */
public final class ComposedSqsMessage<T> extends AbstractSqsMessage<T> {

    private final String messageDeduplicationId;

    private final String messageGroupId;

    private final Integer senderDelaySeconds;

    private ComposedSqsMessage(
            String messageDeduplicationId,
            String messageGroupId,
            Map<String, MessageAttributeValue> messageAttributes,
            Map<String, MessageSystemAttributeValue> messageSystemAttributes,
            T body,
            Integer senderDelaySeconds) {
        super(messageAttributes, messageSystemAttributes, body);
        this.messageDeduplicationId = messageDeduplicationId;
        this.messageGroupId = messageGroupId;
        this.senderDelaySeconds = senderDelaySeconds;
    }

    public static <T> SqsMessage<T> fromBody(T body) {
        return newBuilder(body).build();
    }

    public static <T> Builder<T> newBuilder(T body) {
        return ComposedSqsMessage.<T>newBuilder().body(body);
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
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
        return Optional.ofNullable(senderDelaySeconds);
    }

    public static final class Builder<T> {

        private String messageDeduplicationId;

        private String messageGroupId;

        private Map<String, MessageAttributeValue> messageAttributes = Collections.emptyMap();

        private Map<String, MessageSystemAttributeValue> messageSystemAttributes = Collections.emptyMap();

        private T body;

        private Integer senderDelaySeconds;

        private Builder() {}

        public ComposedSqsMessage<T> build() {
            return new ComposedSqsMessage<>(
                    messageDeduplicationId,
                    messageGroupId,
                    messageAttributes,
                    messageSystemAttributes,
                    body,
                    senderDelaySeconds);
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

        public Builder<T> messageSystemAttributes(Map<String, MessageSystemAttributeValue> messageSystemAttributes) {
            this.messageSystemAttributes = messageSystemAttributes;
            return this;
        }

        public Builder<T> body(T body) {
            this.body = body;
            return this;
        }

        public Builder<T> senderDelaySeconds(Integer senderDelaySeconds) {
            this.senderDelaySeconds = senderDelaySeconds;
            return this;
        }
    }
}
