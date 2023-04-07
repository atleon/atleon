package io.atleon.opentracing;

import com.rabbitmq.client.AMQP;
import io.atleon.core.Alo;
import io.atleon.rabbitmq.AloReceivedRabbitMQMessageDecorator;
import io.atleon.rabbitmq.ReceivedRabbitMQMessage;
import io.atleon.util.ConfigLoading;
import io.opentracing.Tracer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An {@link AloReceivedRabbitMQMessageDecorator} that decorates {@link Alo} elements with tracing
 * context extracted from {@link ReceivedRabbitMQMessage}s
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link ReceivedRabbitMQMessage}s
 */
public class TracingAloReceivedRabbitMQMessageDecorator<T>
    extends TracingAloConsumptionDecorator<ReceivedRabbitMQMessage<T>>
    implements AloReceivedRabbitMQMessageDecorator<T> {

    private String queue = null;

    @Override
    public void configure(Map<String, ?> properties) {
        super.configure(properties);
        this.queue = ConfigLoading.loadString(properties, QUEUE_CONFIG).orElse(queue);
    }

    @Override
    protected Tracer.SpanBuilder newSpanBuilder(SpanBuilderFactory spanBuilderFactory, ReceivedRabbitMQMessage<T> message) {
        return spanBuilderFactory.newSpanBuilder("atleon.rabbitmq.consume")
            .withTag("queue", queue)
            .withTag("exchange", message.getExchange())
            .withTag("routing_key", message.getRoutingKey());
    }

    @Override
    protected Map<String, String> extractHeaderMap(ReceivedRabbitMQMessage<T> message) {
        Map<String, Object> headers = Optional.ofNullable(message.getProperties())
            .map(AMQP.BasicProperties::getHeaders)
            .orElse(Collections.emptyMap());
        return headers.entrySet().stream()
            .filter(entry -> entry.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
    }
}
