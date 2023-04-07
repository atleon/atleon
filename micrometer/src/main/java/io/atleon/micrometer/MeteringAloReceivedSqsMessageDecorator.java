package io.atleon.micrometer;

import io.atleon.aws.sqs.AloReceivedSqsMessageDecorator;
import io.atleon.aws.sqs.ReceivedSqsMessage;
import io.atleon.core.Alo;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tags;

import java.util.Map;
import java.util.Objects;

/**
 * An {@link AloReceivedSqsMessageDecorator} that decorates {@link Alo} elements with metering
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedSqsMessage}s
 */
public class MeteringAloReceivedSqsMessageDecorator<T>
    extends MeteringAloDecorator<ReceivedSqsMessage<T>>
    implements AloReceivedSqsMessageDecorator<T> {

    private String queueUrl = null;

    public MeteringAloReceivedSqsMessageDecorator() {
        super("atleon.alo.sqs.receive");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        this.queueUrl = ConfigLoading.loadString(properties, QUEUE_URL_CONFIG).orElse(queueUrl);
    }

    @Override
    protected Tags extractTags(ReceivedSqsMessage<T> receivedSqsMessage) {
        return Tags.of("queue_url", Objects.toString(queueUrl));
    }
}
