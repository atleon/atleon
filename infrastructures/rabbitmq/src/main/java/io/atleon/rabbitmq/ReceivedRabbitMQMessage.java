package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

public class ReceivedRabbitMQMessage<T> extends RabbitMQMessage<T> {

    private final boolean redeliver;

    private ReceivedRabbitMQMessage(
            String exchange, String routingKey, AMQP.BasicProperties properties, T body, boolean redeliver) {
        super(exchange, routingKey, properties, body);
        this.redeliver = redeliver;
    }

    public static <T> ReceivedRabbitMQMessage<T> create(
            String exchange, String routingKey, AMQP.BasicProperties properties, T body, boolean redeliver) {
        return new ReceivedRabbitMQMessage<>(exchange, routingKey, properties, body, redeliver);
    }

    public boolean redeliver() {
        return redeliver;
    }

    /**
     * @deprecated Use {@link #redeliver()}
     */
    @Deprecated
    public boolean isRedeliver() {
        return redeliver;
    }
}
