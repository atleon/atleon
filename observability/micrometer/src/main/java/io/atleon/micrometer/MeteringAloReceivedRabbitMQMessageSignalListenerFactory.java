package io.atleon.micrometer;

import io.atleon.rabbitmq.AloReceivedRabbitMQMessageSignalListenerFactory;
import io.atleon.rabbitmq.ReceivedRabbitMQMessage;
import io.atleon.util.ConfigLoading;
import io.micrometer.core.instrument.Tags;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * An {@link AloReceivedRabbitMQMessageSignalListenerFactory} that creates
 * {@link reactor.core.observability.SignalListener} instances which apply metering to Reactor
 * Publishers of {@link io.atleon.core.Alo} items referencing Kafka {@link ReceivedRabbitMQMessage}.
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedRabbitMQMessage}s
 */
public final class MeteringAloReceivedRabbitMQMessageSignalListenerFactory<T>
        extends MeteringAloSignalListenerFactory<ReceivedRabbitMQMessage<T>, Void>
        implements AloReceivedRabbitMQMessageSignalListenerFactory<T, Void> {

    private String queue = null;

    public MeteringAloReceivedRabbitMQMessageSignalListenerFactory() {
        super("atleon.alo.publisher.signal.receive.rabbitmq");
    }

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        this.queue = ConfigLoading.loadString(properties, QUEUE_CONFIG).orElse(queue);
    }

    @Override
    protected Function<? super ReceivedRabbitMQMessage<T>, Void> keyExtractor() {
        return message -> null;
    }

    @Override
    protected Tagger<? super Void> tagger() {
        return Tagger.composed(Tags.of("queue", Objects.toString(queue)), __ -> Tags.empty());
    }
}
