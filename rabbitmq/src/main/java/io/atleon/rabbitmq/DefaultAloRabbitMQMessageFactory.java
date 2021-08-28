package io.atleon.rabbitmq;

import io.atleon.core.Alo;

import java.util.Map;
import java.util.function.Consumer;

public class DefaultAloRabbitMQMessageFactory<T> implements AloRabbitMQMessageFactory<T> {

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Alo<RabbitMQMessage<T>>
    create(RabbitMQMessage<T> rabbitMQMessage, Runnable acknowledger, Consumer<? super Throwable> nacknowedger) {
        return new DefaultAloRabbitMQMessage<>(rabbitMQMessage, acknowledger, nacknowedger);
    }
}
