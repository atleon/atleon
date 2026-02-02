package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

/**
 * A message received from RabbitMQ that has been emitted and requires settlement after processing.
 * Settlement may either be "ack" (for successful processing), "reject" (for permanent/fatally
 * failed processing), or "requeue" (for future retrial). Settlement is idempotent, and once
 * invoked, subsequent invocations will have no effect.
 */
public final class RabbitMQReceiverMessage {

    private final boolean redeliver;

    private final String exchange;

    private final String routingKey;

    private final AMQP.BasicProperties properties;

    private final byte[] body;

    private final RabbitMQDeliverySettler deliverySettler;

    private RabbitMQReceiverMessage(
            boolean redeliver,
            String exchange,
            String routingKey,
            AMQP.BasicProperties properties,
            byte[] body,
            RabbitMQDeliverySettler deliverySettler) {
        this.redeliver = redeliver;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.properties = properties;
        this.body = body;
        this.deliverySettler = deliverySettler;
    }

    public static RabbitMQReceiverMessage create(
            Envelope envelope, AMQP.BasicProperties properties, byte[] body, RabbitMQDeliverySettler deliverySettler) {
        return new RabbitMQReceiverMessage(
                envelope.isRedeliver(),
                envelope.getExchange(),
                envelope.getRoutingKey(),
                properties,
                body,
                deliverySettler);
    }

    public boolean redeliver() {
        return redeliver;
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

    public byte[] body() {
        return body;
    }

    public void settleWithAck() {
        deliverySettler.ack();
    }

    public void settleWithReject() {
        deliverySettler.reject();
    }

    public void settleWithRequeue() {
        deliverySettler.requeue();
    }

    public RabbitMQDeliverySettler deliverySettler() {
        return deliverySettler;
    }
}
