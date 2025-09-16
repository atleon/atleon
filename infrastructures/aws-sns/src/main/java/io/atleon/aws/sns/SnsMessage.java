package io.atleon.aws.sns;

import software.amazon.awssdk.services.sns.model.MessageAttributeValue;

import java.util.Map;
import java.util.Optional;

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
