package io.atleon.micrometer;

import io.atleon.aws.sqs.AloReceivedSqsMessageSignalListener;
import io.atleon.aws.sqs.ReceivedSqsMessage;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tag;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link AloReceivedSqsMessageSignalListener} that applies metering to emitted
 * {@link reactor.core.publisher.Signal}s referencing {@link io.atleon.core.Alo} of
 * {@link ReceivedSqsMessage}.
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedSqsMessage}s
 */
public class MeteringAloReceivedSqsMessageSignalListener<T>
    extends MeteringAloSignalListener<ReceivedSqsMessage<T>>
    implements AloReceivedSqsMessageSignalListener<T> {

    private String queueUrl = null;

    public MeteringAloReceivedSqsMessageSignalListener() {
        super("atleon.alo.publisher.signal.sqs.receive");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        this.queueUrl = ConfigLoading.loadString(properties, QUEUE_URL_CONFIG).orElse(queueUrl);
    }

    @Override
    protected Iterable<Tag> baseTags() {
        return Collections.singletonList(Tag.of("queue_url", Objects.toString(queueUrl)));
    }
}
