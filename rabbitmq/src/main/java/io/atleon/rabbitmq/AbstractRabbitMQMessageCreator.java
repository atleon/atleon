package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

import java.util.UUID;

public abstract class AbstractRabbitMQMessageCreator<T> implements RabbitMQMessageCreator<T> {

    private final AMQP.BasicProperties initialProperties;

    public AbstractRabbitMQMessageCreator(AMQP.BasicProperties initialProperties) {
        this.initialProperties = initialProperties;
    }

    @Override
    public RabbitMQMessage<T> apply(T t) {
        return new RabbitMQMessage<>(
            extractExchange(t),
            extractRoutingKey(t),
            createMessagePropertiesBuilder(t).build(),
            t);
    }

    protected abstract String extractExchange(T t);

    protected abstract String extractRoutingKey(T t);

    protected AMQP.BasicProperties.Builder createMessagePropertiesBuilder(T t) {
        return initialProperties.builder().messageId(UUID.randomUUID().toString());
    }
}
