package io.atleon.micrometer;

import io.atleon.core.Alo;
import io.atleon.rabbitmq.AloReceivedRabbitMQMessageDecorator;
import io.atleon.rabbitmq.ReceivedRabbitMQMessage;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import java.util.Map;
import java.util.Objects;

/**
 * An {@link AloReceivedRabbitMQMessageDecorator} that decorates {@link Alo} elements with metering
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedRabbitMQMessage}s
 */
public final class MeteringAloReceivedRabbitMQMessageDecorator<T>
    extends MeteringAloDecorator<ReceivedRabbitMQMessage<T>, Void>
    implements AloReceivedRabbitMQMessageDecorator<T> {

    private String queue = null;

    public MeteringAloReceivedRabbitMQMessageDecorator() {
        super("atleon.alo.receive.rabbitmq");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        this.queue = ConfigLoading.loadString(properties, QUEUE_CONFIG).orElse(queue);
    }

    @Override
    protected Void extractKey(ReceivedRabbitMQMessage<T> receivedRabbitMQMessage) {
        return null;
    }

    @Override
    protected Iterable<Tag> extractTags(Void key) {
        return Tags.of("queue", Objects.toString(queue));
    }
}
