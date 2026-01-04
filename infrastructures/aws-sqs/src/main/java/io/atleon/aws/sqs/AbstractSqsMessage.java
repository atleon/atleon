package io.atleon.aws.sqs;

import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue;

/**
 * Base implementation of an {@link SqsMessage}
 *
 * @param <T> The type of the body held by this Message
 */
public abstract class AbstractSqsMessage<T> implements SqsMessage<T> {

    private final Map<String, MessageAttributeValue> messageAttributes;

    private final Map<String, MessageSystemAttributeValue> messageSystemAttributes;

    private final T body;

    protected AbstractSqsMessage(
            Map<String, MessageAttributeValue> messageAttributes,
            Map<String, MessageSystemAttributeValue> messageSystemAttributes,
            T body) {
        this.messageAttributes = messageAttributes;
        this.messageSystemAttributes = messageSystemAttributes;
        this.body = body;
    }

    @Override
    public final Map<String, MessageAttributeValue> messageAttributes() {
        return messageAttributes;
    }

    public final Optional<MessageSystemAttributeValue> messageSystemAttribute(MessageSystemAttributeName key) {
        return messageSystemAttribute(key.toString());
    }

    public final Optional<MessageSystemAttributeValue> messageSystemAttribute(String key) {
        return Optional.ofNullable(messageSystemAttributes.get(key));
    }

    @Override
    public final Map<String, MessageSystemAttributeValue> messageSystemAttributes() {
        return messageSystemAttributes;
    }

    @Override
    public final T body() {
        return body;
    }
}
