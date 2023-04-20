package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

public class ReceivedRabbitMQMessage<T> extends RabbitMQMessage<T> {

    private final boolean redeliver;

    public ReceivedRabbitMQMessage(
        String exchange,
        String routingKey,
        AMQP.BasicProperties properties,
        T body,
        boolean redeliver
    ) {
        super(exchange, routingKey, properties, body);
        this.redeliver = redeliver;
    }

    public boolean isRedeliver() {
        return redeliver;
    }
}
