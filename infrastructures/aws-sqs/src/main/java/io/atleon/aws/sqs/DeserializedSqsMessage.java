package io.atleon.aws.sqs;

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue;

import java.util.Map;
import java.util.Optional;

/**
 * An inbound {@link SqsMessage} whose body has been deserialized.
 *
 * @param <T> The (deserialized) type of the body held by this Message
 */
public final class DeserializedSqsMessage<T> extends AbstractSqsMessage<T> implements ReceivedSqsMessage<T> {

    private final String receiptHandle;

    private final String messageId;

    private DeserializedSqsMessage(
            String receiptHandle,
            String messageId,
            Map<String, MessageAttributeValue> messageAttributes,
            Map<String, MessageSystemAttributeValue> messageSystemAttributes,
            T body) {
        super(messageAttributes, messageSystemAttributes, body);
        this.receiptHandle = receiptHandle;
        this.messageId = messageId;
    }

    public static <T> DeserializedSqsMessage<T> deserialize(
            ReceivedSqsMessage<String> serializedMessage, BodyDeserializer<T> bodyDeserializer) {
        return new DeserializedSqsMessage<>(
                serializedMessage.receiptHandle(),
                serializedMessage.messageId(),
                serializedMessage.messageAttributes(),
                serializedMessage.messageSystemAttributes(),
                bodyDeserializer.deserialize(serializedMessage.body()));
    }

    @Override
    public String messageId() {
        return messageId;
    }

    @Override
    public String receiptHandle() {
        return receiptHandle;
    }

    @Override
    public Optional<String> messageDeduplicationId() {
        return messageSystemAttribute(MessageSystemAttributeName.MESSAGE_DEDUPLICATION_ID)
                .map(MessageSystemAttributeValue::stringValue);
    }

    @Override
    public Optional<String> messageGroupId() {
        return messageSystemAttribute(MessageSystemAttributeName.MESSAGE_GROUP_ID)
                .map(MessageSystemAttributeValue::stringValue);
    }
}
