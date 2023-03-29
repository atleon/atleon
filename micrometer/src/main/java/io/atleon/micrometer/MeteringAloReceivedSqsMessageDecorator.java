package io.atleon.micrometer;

import io.atleon.aws.sqs.AloReceivedSqsMessageDecorator;
import io.atleon.aws.sqs.ReceivedSqsMessage;
import io.atleon.core.Alo;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

/**
 * An {@link AloReceivedSqsMessageDecorator} that decorates {@link Alo} elements with metering
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedSqsMessage}s
 */
public class MeteringAloReceivedSqsMessageDecorator<T>
    extends MeteringAloDecorator<ReceivedSqsMessage<T>>
    implements AloReceivedSqsMessageDecorator<T> {

    @Override
    protected Tags extractTags(ReceivedSqsMessage<T> receivedSqsMessage) {
        return Tags.of(
            Tag.of("type", "sqs"),
            Tag.of("queueUrl", receivedSqsMessage.queueUrl())
        );
    }
}
