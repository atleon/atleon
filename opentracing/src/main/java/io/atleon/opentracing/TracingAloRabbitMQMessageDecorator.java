package io.atleon.opentracing;

import com.rabbitmq.client.AMQP;
import io.atleon.core.Alo;
import io.atleon.rabbitmq.AloRabbitMQMessageDecorator;
import io.atleon.rabbitmq.RabbitMQMessage;
import io.opentracing.References;
import io.opentracing.SpanContext;
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
public class TracingAloRabbitMQMessageDecorator<T> extends ConsumerTracing implements AloRabbitMQMessageDecorator<T> {

    @Override
    public Alo<RabbitMQMessage<T>> decorate(Alo<RabbitMQMessage<T>> alo) {
        RabbitMQMessage<T> rabbitMQMessage = alo.get();
        Tracer.SpanBuilder spanBuilder = newSpanBuilder("atleon.rabbitmq.consume")
            .withTag("exchange", rabbitMQMessage.getExchange())
            .withTag("routingKey", rabbitMQMessage.getRoutingKey());
        extractSpanContext(rabbitMQMessage)
            .ifPresent(it -> spanBuilder.addReference(References.FOLLOWS_FROM, it));
        return TracingAlo.start(alo, tracerFacade(), spanBuilder);
    }

    protected Optional<SpanContext> extractSpanContext(RabbitMQMessage<?> rabbitMQMessage) {
        Map<String, Object> headers = Optional.ofNullable(rabbitMQMessage.getProperties())
            .map(AMQP.BasicProperties::getHeaders)
            .orElse(Collections.emptyMap());
        Map<String, String> headerMap = headers.entrySet().stream()
            .filter(entry -> entry.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
        return extractSpanContext(headerMap);
    }
}
