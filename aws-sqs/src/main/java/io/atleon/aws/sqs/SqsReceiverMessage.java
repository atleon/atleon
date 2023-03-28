package io.atleon.aws.sqs;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An {@link SqsMessage} that has been received and must have its deletion and visibility
 * explicitly handled.
 */
public final class SqsReceiverMessage extends AbstractSqsMessage<String> implements ReceivedSqsMessage<String> {

    private final String queueUrl;

    private final String receiptHandle;

    private final String messageId;

    private final Runnable deleter;

    private final SqsMessageVisibilityChanger visibilityChanger;

    private SqsReceiverMessage(
        String queueUrl,
        String receiptHandle,
        String messageId,
        Map<String, MessageAttributeValue> messageAttributes,
        Map<String, MessageSystemAttributeValue> messageSystemAttributes,
        String body,
        Runnable deleter,
        SqsMessageVisibilityChanger visibilityChanger
    ) {
        super(messageAttributes, messageSystemAttributes, body);
        this.queueUrl = queueUrl;
        this.receiptHandle = receiptHandle;
        this.messageId = messageId;
        this.deleter = deleter;
        this.visibilityChanger = visibilityChanger;
    }

    static SqsReceiverMessage
    create(String queueUrl, Message message, Runnable deleter, SqsMessageVisibilityChanger visibilityChanger) {
        return new SqsReceiverMessage(
            queueUrl,
            message.receiptHandle(),
            message.messageId(),
            message.messageAttributes(),
            toMessageSystemAttributes(message.attributesAsStrings()),
            message.body(),
            deleter,
            visibilityChanger
        );
    }

    @Override
    public String queueUrl() {
        return queueUrl;
    }

    @Override
    public String receiptHandle() {
        return receiptHandle;
    }

    @Override
    public String messageId() {
        return messageId;
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

    /**
     * Schedules this Message for deletion
     */
    public void delete() {
        deleter.run();
    }

    /**
     * Returns the {@link Runnable} used to schedule this Message for deletion.
     */
    public Runnable deleter() {
        return deleter;
    }

    /**
     * Schedules a change of this Message's visibility by the provided Duration (which should
     * have a whole number of seconds) and marks the message as no longer in flight.
     */
    public void changeVisibility(Duration timeout) {
        changeVisibility(timeout, false);
    }

    /**
     * Schedules a change of this Message's visibility by the provided Duration (which should
     * have a whole number of seconds) and may mark the message as still in flight.
     */
    public void changeVisibility(Duration timeout, boolean stillInFlight) {
        visibilityChanger.execute(timeout, stillInFlight);
    }

    /**
     * Returns the {@link SqsMessageVisibilityChanger} used to explicitly manage this Message's
     * visibility.
     */
    public SqsMessageVisibilityChanger visibilityChanger() {
        return visibilityChanger;
    }

    private static Map<String, MessageSystemAttributeValue> toMessageSystemAttributes(
        Map<String, String> messageSystemAttributesAsStrings
    ) {
        return messageSystemAttributesAsStrings.entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> toMessageSystemAttributeValue(entry.getValue())
            )
        );
    }

    private static MessageSystemAttributeValue toMessageSystemAttributeValue(String value) {
        return MessageSystemAttributeValue.builder().stringValue(value).build();
    }
}
