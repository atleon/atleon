package io.atleon.aws.sqs;

/**
 * An inbound {@link SqsMessage}. Such Messages are guaranteed to have a Receipt Handle and Message
 * ID.
 *
 * @param <T> The type of the body referenced by this Message
 */
public interface ReceivedSqsMessage<T> extends SqsMessage<T> {

    /**
     * The URL of the queue from which this message was received.
     */
    String queueUrl();

    /**
     * The Receipt Handle of this message, used to manage its visibility and deletion. Note that
     * this value is unique to the act of receiving a Message with any given ID, and a unique
     * Message with any given ID will receive a unique Receipt Handle each time it is received.
     */
    String receiptHandle();

    /**
     * Unique identifier for this Message.
     */
    String messageId();
}
