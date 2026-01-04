package io.atleon.micrometer;

import io.atleon.aws.sqs.AloReceivedSqsMessageSignalListenerFactory;
import io.atleon.aws.sqs.ReceivedSqsMessage;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tags;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * An {@link AloReceivedSqsMessageSignalListenerFactory} that creates
 * {@link reactor.core.observability.SignalListener} instances which apply metering to Reactor
 * Publishers of {@link io.atleon.core.Alo} items referencing Kafka {@link ReceivedSqsMessage}.
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedSqsMessage}s
 */
public final class MeteringAloReceivedSqsMessageSignalListenerFactory<T>
        extends MeteringAloSignalListenerFactory<ReceivedSqsMessage<T>, Void>
        implements AloReceivedSqsMessageSignalListenerFactory<T, Void> {

    private String queueUrl = null;

    public MeteringAloReceivedSqsMessageSignalListenerFactory() {
        super("atleon.alo.publisher.signal.receive.aws.sqs");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        this.queueUrl = ConfigLoading.loadString(properties, QUEUE_URL_CONFIG).orElse(queueUrl);
    }

    @Override
    protected Function<? super ReceivedSqsMessage<T>, Void> keyExtractor() {
        return message -> null;
    }

    @Override
    protected Tagger<? super Void> tagger() {
        return Tagger.composed(Tags.of("queue_url", Objects.toString(queueUrl)), __ -> Tags.empty());
    }
}
