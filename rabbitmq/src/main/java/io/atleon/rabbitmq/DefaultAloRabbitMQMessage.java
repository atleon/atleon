package io.atleon.rabbitmq;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.ComposedAlo;

import java.util.function.Consumer;

public class DefaultAloRabbitMQMessage<T> implements Alo<RabbitMQMessage<T>> {

    private final RabbitMQMessage<T> rabbitMQMessage;

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    public DefaultAloRabbitMQMessage(
        RabbitMQMessage<T> rabbitMQMessage,
        Runnable acknowledger,
        Consumer<? super Throwable> nacknowledger) {
        this.rabbitMQMessage = rabbitMQMessage;
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return ComposedAlo::new;
    }

    @Override
    public RabbitMQMessage<T> get() {
        return rabbitMQMessage;
    }

    @Override
    public Runnable getAcknowledger() {
        return acknowledger;
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return nacknowledger;
    }
}
