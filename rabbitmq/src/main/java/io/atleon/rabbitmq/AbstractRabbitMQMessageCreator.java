package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

import java.util.UUID;

public abstract class AbstractRabbitMQMessageCreator<T> implements RabbitMQMessageCreator<T> {

    private final AMQP.BasicProperties initialProperties;

    public AbstractRabbitMQMessageCreator(AMQP.BasicProperties initialProperties) {
        this.initialProperties = initialProperties;
    }

    @Override
    public RabbitMQMessage<T> apply(T body) {
        return new RabbitMQMessage<>(
            extractExchange(body),
            extractRoutingKey(body),
            createMessagePropertiesBuilder(body).build(),
            body);
    }

    protected abstract String extractExchange(T body);

    protected abstract String extractRoutingKey(T body);

    protected AMQP.BasicProperties.Builder createMessagePropertiesBuilder(T body) {
        return initialProperties.builder().messageId(UUID.randomUUID().toString());
    }
}
