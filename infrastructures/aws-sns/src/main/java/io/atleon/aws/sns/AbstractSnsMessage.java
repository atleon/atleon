package io.atleon.aws.sns;

import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;

/**
 * Base implementation of an {@link SnsMessage}
 *
 * @param <T> The type of the body held by this Message
 */
public abstract class AbstractSnsMessage<T> implements SnsMessage<T> {

    private final String messageDeduplicationId;

    private final String messageGroupId;

    private final Map<String, MessageAttributeValue> messageAttributes;

    private final String messageStructure;

    private final String subject;

    private final T body;

    protected AbstractSnsMessage(
            String messageDeduplicationId,
            String messageGroupId,
            Map<String, MessageAttributeValue> messageAttributes,
            String messageStructure,
            String subject,
            T body) {
        this.messageDeduplicationId = messageDeduplicationId;
        this.messageGroupId = messageGroupId;
        this.messageAttributes = messageAttributes;
        this.messageStructure = messageStructure;
        this.subject = subject;
        this.body = body;
    }

    @Override
    public final Optional<String> messageDeduplicationId() {
        return Optional.ofNullable(messageDeduplicationId);
    }

    @Override
    public final Optional<String> messageGroupId() {
        return Optional.ofNullable(messageGroupId);
    }

    @Override
    public final Map<String, MessageAttributeValue> messageAttributes() {
        return messageAttributes;
    }

    @Override
    public final Optional<String> messageStructure() {
        return Optional.ofNullable(messageStructure);
    }

    @Override
    public final Optional<String> subject() {
        return Optional.ofNullable(subject);
    }

    @Override
    public final T body() {
        return body;
    }
}
