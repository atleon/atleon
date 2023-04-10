package io.atleon.micrometer;

import io.atleon.rabbitmq.AloReceivedRabbitMQMessageSignalObserver;
import io.atleon.rabbitmq.ReceivedRabbitMQMessage;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import java.util.Map;
import java.util.Objects;

/**
 * An {@link AloReceivedRabbitMQMessageSignalObserver} that applies metering to emitted
 * {@link reactor.core.publisher.Signal}s referencing {@link io.atleon.core.Alo} of
 * {@link ReceivedRabbitMQMessage}.
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedRabbitMQMessage}s
 */
public final class MeteringAloReceivedRabbitMQMessageSignalObserver<T>
    extends MeteringAloSignalObserver<ReceivedRabbitMQMessage<T>, Void>
    implements AloReceivedRabbitMQMessageSignalObserver<T> {

    private String queue = null;

    public MeteringAloReceivedRabbitMQMessageSignalObserver() {
        super("atleon.alo.publisher.signal.receive.rabbitmq");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        this.queue = ConfigLoading.loadString(properties, QUEUE_CONFIG).orElse(queue);
    }

    @Override
    protected Void extractKey(ReceivedRabbitMQMessage<T> value) {
        return null;
    }

    @Override
    protected Iterable<Tag> baseTags() {
        return Tags.of("queue", Objects.toString(queue));
    }

    @Override
    protected Iterable<Tag> extractTags(Void key) {
        return Tags.empty();
    }
}
