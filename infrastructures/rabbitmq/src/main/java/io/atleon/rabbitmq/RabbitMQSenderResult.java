package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;
import io.atleon.core.Alo;
import io.atleon.core.SenderResult;
import reactor.rabbitmq.CorrelableOutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;

import java.util.Optional;

public final class RabbitMQSenderResult<T> implements SenderResult {

    private final String exchange;

    private final String routingKey;

    private final AMQP.BasicProperties properties;

    private final T correlationMetadata;

    private final Throwable error;

    private RabbitMQSenderResult(
            String exchange,
            String routingKey,
            AMQP.BasicProperties properties,
            T correlationMetadata,
            Throwable error) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.properties = properties;
        this.correlationMetadata = correlationMetadata;
        this.error = error;
    }

    static <T> RabbitMQSenderResult<T> success(OutboundDelivery<T> outboundDelivery) {
        return new RabbitMQSenderResult<>(
                outboundDelivery.exchange(),
                outboundDelivery.routingKey(),
                outboundDelivery.properties(),
                outboundDelivery.correlationMetadata(),
                null);
    }

    static <T> RabbitMQSenderResult<T> failure(OutboundDelivery<T> outboundDelivery, Throwable error) {
        return new RabbitMQSenderResult<>(
                outboundDelivery.exchange(),
                outboundDelivery.routingKey(),
                outboundDelivery.properties(),
                outboundDelivery.correlationMetadata(),
                error);
    }

    static <T> Alo<RabbitMQSenderResult<T>> invertAlo(RabbitMQSenderResult<Alo<T>> senderResult) {
        return senderResult
                .correlationMetadata()
                .map(correlationMetadata -> new RabbitMQSenderResult<>(
                        senderResult.exchange,
                        senderResult.routingKey,
                        senderResult.properties,
                        correlationMetadata,
                        senderResult.error));
    }

    static <T> RabbitMQSenderResult<T> fromMessageResult(
            OutboundMessageResult<CorrelableOutboundMessage<T>> messageResult) {
        return new RabbitMQSenderResult<>(
                messageResult.getOutboundMessage().getExchange(),
                messageResult.getOutboundMessage().getRoutingKey(),
                messageResult.getOutboundMessage().getProperties(),
                messageResult.getOutboundMessage().getCorrelationMetadata(),
                messageResult.isAck() ? null : new UnackedRabbitMQMessageException());
    }

    static <T> Alo<RabbitMQSenderResult<T>> fromMessageResultOfAlo(
            OutboundMessageResult<CorrelableOutboundMessage<Alo<T>>> messageResult) {
        return messageResult
                .getOutboundMessage()
                .getCorrelationMetadata()
                .map(correlationMetadata -> new RabbitMQSenderResult<>(
                        messageResult.getOutboundMessage().getExchange(),
                        messageResult.getOutboundMessage().getRoutingKey(),
                        messageResult.getOutboundMessage().getProperties(),
                        correlationMetadata,
                        messageResult.isAck() ? null : new UnackedRabbitMQMessageException()));
    }

    @Override
    public Optional<Throwable> failureCause() {
        return Optional.ofNullable(error);
    }

    @Override
    public String toString() {
        return "RabbitMQSenderResult{" + "exchange='"
                + exchange + '\'' + ", routingKey='"
                + routingKey + '\'' + ", properties="
                + properties + ", correlationMetadata="
                + correlationMetadata + ", error="
                + error + '}';
    }

    public String exchange() {
        return exchange;
    }

    public String routingKey() {
        return routingKey;
    }

    public AMQP.BasicProperties properties() {
        return properties;
    }

    public T correlationMetadata() {
        return correlationMetadata;
    }

    /**
     * @deprecated Use {@link #exchange()}
     */
    @Deprecated
    public String getExchange() {
        return exchange;
    }

    /**
     * @deprecated Use {@link #routingKey()}
     */
    @Deprecated
    public String getRoutingKey() {
        return routingKey;
    }

    /**
     * @deprecated Use {@link #properties()}
     */
    @Deprecated
    public AMQP.BasicProperties getProperties() {
        return properties;
    }

    /**
     * @deprecated Use {@link #correlationMetadata()}
     */
    @Deprecated
    public T getCorrelationMetadata() {
        return correlationMetadata;
    }

    /**
     * @deprecated Use complement of {@link #isFailure()}
     */
    @Deprecated
    public boolean isAck() {
        return error == null;
    }
}
