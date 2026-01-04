package io.atleon.aws.sns;

import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;

/**
 * The base interface of all SNS Messages
 *
 * @param <T> The type of the body referenced by this Message
 */
public interface SnsMessage<T> {

    Optional<String> messageDeduplicationId();

    Optional<String> messageGroupId();

    Map<String, MessageAttributeValue> messageAttributes();

    Optional<String> messageStructure();

    Optional<String> subject();

    T body();
}
