package io.atleon.micrometer;

import io.atleon.rabbitmq.AloReceivedRabbitMQMessageSignalListener;
import io.atleon.rabbitmq.ReceivedRabbitMQMessage;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tag;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link AloReceivedRabbitMQMessageSignalListener} that applies metering to emitted
 * {@link reactor.core.publisher.Signal}s referencing {@link io.atleon.core.Alo} of
 * {@link ReceivedRabbitMQMessage}.
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedRabbitMQMessage}s
 */
public class MeteringAloReceivedRabbitMQMessageSignalListener<T>
    extends MeteringAloSignalListener<ReceivedRabbitMQMessage<T>>
    implements AloReceivedRabbitMQMessageSignalListener<T> {

    private String queue = null;

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        this.queue = ConfigLoading.loadString(properties, QUEUE_CONFIG).orElse(queue);
    }

    @Override
    protected Iterable<Tag> baseTags() {
        return Arrays.asList(
            Tag.of("source", "rabbitmq-receive"),
            Tag.of("queue", Objects.toString(queue))
        );
    }
}
