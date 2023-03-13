package io.atleon.opentracing;

import com.rabbitmq.client.AMQP;
import io.atleon.core.Alo;
import io.atleon.rabbitmq.AloRabbitMQMessageDecorator;
import io.atleon.rabbitmq.RabbitMQMessage;
import io.opentracing.Tracer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An {@link AloRabbitMQMessageDecorator} that decorates {@link Alo} elements with tracing
 * context extracted from {@link RabbitMQMessage}s
 *
 * @param <T> The types of (deserialized) body payloads referenced by {@link RabbitMQMessage}s
 */
public class TracingAloRabbitMQMessageDecorator<T>
    extends TracingAloConsumptionDecorator<RabbitMQMessage<T>>
    implements AloRabbitMQMessageDecorator<T> {

    @Override
    protected Tracer.SpanBuilder newSpanBuilder(SpanBuilderFactory spanBuilderFactory, RabbitMQMessage<T> message) {
        return spanBuilderFactory.newSpanBuilder("atleon.rabbitmq.consume")
            .withTag("exchange", message.getExchange())
            .withTag("routingKey", message.getRoutingKey());
    }

    @Override
    protected Map<String, String> extractHeaderMap(RabbitMQMessage<T> message) {
        Map<String, Object> headers = Optional.ofNullable(message.getProperties())
            .map(AMQP.BasicProperties::getHeaders)
            .orElse(Collections.emptyMap());
        return headers.entrySet().stream()
            .filter(entry -> entry.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
    }
}
