package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;
import io.atleon.core.Alo;
import io.atleon.core.SenderResult;
import reactor.rabbitmq.CorrelableOutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;

import java.util.Optional;

public class RabbitMQSenderResult<T> implements SenderResult {

    private final String exchange;

    private final String routingKey;

    private final AMQP.BasicProperties properties;

    private final T correlationMetadata;

    private final boolean ack;

    private RabbitMQSenderResult(
        String exchange,
        String routingKey,
        AMQP.BasicProperties properties,
        T correlationMetadata,
        boolean ack) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.properties = properties;
        this.correlationMetadata = correlationMetadata;
        this.ack = ack;
    }

    static <T> RabbitMQSenderResult<T>
    fromMessageResult(OutboundMessageResult<CorrelableOutboundMessage<T>> messageResult) {
        return new RabbitMQSenderResult<>(
            messageResult.getOutboundMessage().getExchange(),
            messageResult.getOutboundMessage().getRoutingKey(),
            messageResult.getOutboundMessage().getProperties(),
            messageResult.getOutboundMessage().getCorrelationMetadata(),
            messageResult.isAck());
    }

    static <T> Alo<RabbitMQSenderResult<T>>
    fromMessageResultOfAlo(OutboundMessageResult<CorrelableOutboundMessage<Alo<T>>> messageResult) {
        return messageResult.getOutboundMessage().getCorrelationMetadata().map(correlationMetadata ->
            new RabbitMQSenderResult<>(
                messageResult.getOutboundMessage().getExchange(),
                messageResult.getOutboundMessage().getRoutingKey(),
                messageResult.getOutboundMessage().getProperties(),
                correlationMetadata,
                messageResult.isAck()));
    }

    @Override
    public Optional<Throwable> failureCause() {
        return ack ? Optional.empty() : Optional.of(new UnackedRabbitMQMessageException());
    }

    @Override
    public String toString() {
        return "RabbitMQSenderResult{" +
            "exchange='" + exchange + '\'' +
            ", routingKey='" + routingKey + '\'' +
            ", properties=" + properties +
            ", correlationMetadata=" + correlationMetadata +
            ", ack=" + ack +
            '}';
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public AMQP.BasicProperties getProperties() {
        return properties;
    }

    public T getCorrelationMetadata() {
        return correlationMetadata;
    }

    public boolean isAck() {
        return ack;
    }
}
