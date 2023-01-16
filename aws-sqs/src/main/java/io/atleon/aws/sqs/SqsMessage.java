package io.atleon.aws.sqs;

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeValue;

import java.util.Map;
import java.util.Optional;

/**
 * The base interface of all SQS Messages that may either be inbound (received) or outbound
 * (able to be sent).
 *
 * @param <T> The type of the body referenced by this Message
 */
public interface SqsMessage<T> {

    Optional<String> messageDeduplicationId();

    Optional<String> messageGroupId();

    Map<String, MessageAttributeValue> messageAttributes();

    Map<String, MessageSystemAttributeValue> messageSystemAttributes();

    T body();

    /**
     * When sending this Message, this is the number of seconds to delay its visibility after
     * sending. Only applicable on standard queues. FIFO queues have this parameter set at the
     * queue level.
     *
     * @return Possibly-empty number of seconds to delay this Message's visibility after sending
     */
    default Optional<Integer> senderDelaySeconds() {
        return Optional.empty();
    }
}
