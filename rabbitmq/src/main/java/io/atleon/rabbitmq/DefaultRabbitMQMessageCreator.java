package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.MessageProperties;

import java.util.Objects;
import java.util.function.Function;

public final class DefaultRabbitMQMessageCreator<T> extends AbstractRabbitMQMessageCreator<T> {

    private final Function<? super T, String> exchangeExtractor;

    private final Function<? super T, String> routingKeyExtractor;

    public DefaultRabbitMQMessageCreator(
        AMQP.BasicProperties initialProperties,
        Function<? super T, String> exchangeExtractor,
        Function<? super T, String> routingKeyExtractor) {
        super(initialProperties);
        this.exchangeExtractor = exchangeExtractor;
        this.routingKeyExtractor = routingKeyExtractor;
    }

    public static <T> DefaultRabbitMQMessageCreator<T>
    minimalBasicToDefaultExchange(String queue) {
        return new DefaultRabbitMQMessageCreator<>(MessageProperties.MINIMAL_BASIC, t -> "", t -> queue);
    }

    public static <T> DefaultRabbitMQMessageCreator<T>
    minimalBasicToDefaultExchange(Function<? super T, String> queueExtractor) {
        return new DefaultRabbitMQMessageCreator<>(MessageProperties.MINIMAL_BASIC, t -> "", queueExtractor);
    }

    public static <T> DefaultRabbitMQMessageCreator<T>
    minimalBasic(String exchange) {
        return new DefaultRabbitMQMessageCreator<>(MessageProperties.MINIMAL_BASIC, t -> exchange, t -> "");
    }

    public static <T> DefaultRabbitMQMessageCreator<T>
    minimalBasic(String exchange, String routingKey) {
        return new DefaultRabbitMQMessageCreator<>(MessageProperties.MINIMAL_BASIC, t -> exchange, t -> routingKey);
    }

    public static <T> DefaultRabbitMQMessageCreator<T>
    minimalBasic(String exchange, Function<? super T, String> routingKeyExtractor) {
        return new DefaultRabbitMQMessageCreator<>(MessageProperties.MINIMAL_BASIC, t -> exchange, routingKeyExtractor);
    }

    public static <T> DefaultRabbitMQMessageCreator<T>
    toDefaultExchange(AMQP.BasicProperties properties, String queue) {
        return new DefaultRabbitMQMessageCreator<>(properties, t -> "", t -> queue);
    }

    public static <T> DefaultRabbitMQMessageCreator<T>
    toDefaultExchange(AMQP.BasicProperties properties, Function<? super T, String> queueExtractor) {
        return new DefaultRabbitMQMessageCreator<>(properties, t -> "", queueExtractor);
    }

    public static <T> DefaultRabbitMQMessageCreator<T>
    of(AMQP.BasicProperties properties, String exchange, String routingKey) {
        return new DefaultRabbitMQMessageCreator<>(properties, t -> exchange, t -> routingKey);
    }

    public static <T> DefaultRabbitMQMessageCreator<T>
    of(AMQP.BasicProperties properties, String exchange, Function<? super T, String> routingKeyExtractor) {
        return new DefaultRabbitMQMessageCreator<>(properties, t -> exchange, routingKeyExtractor);
    }

    @Override
    protected String extractExchange(T body) {
        return Objects.toString(exchangeExtractor.apply(body));
    }

    @Override
    protected String extractRoutingKey(T body) {
        return Objects.toString(routingKeyExtractor.apply(body));
    }
}
