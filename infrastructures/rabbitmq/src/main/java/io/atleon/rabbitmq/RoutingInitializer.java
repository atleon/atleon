package io.atleon.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.atleon.util.Throwing;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public final class RoutingInitializer implements Runnable {

    private final ConnectionClosure connectionClosure;

    private final List<ExchangeDeclaration> exchangeDeclarations;

    private final List<QueueDeclaration> queueDeclarations;

    private final List<QueueBinding> queueBindings;

    private RoutingInitializer(ConnectionClosure connectionClosure) {
        this(connectionClosure, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    private RoutingInitializer(
            ConnectionClosure connectionClosure,
            List<ExchangeDeclaration> exchangeDeclarations,
            List<QueueDeclaration> queueDeclarations,
            List<QueueBinding> queueBindings) {
        this.connectionClosure = connectionClosure;
        this.exchangeDeclarations = exchangeDeclarations;
        this.queueDeclarations = queueDeclarations;
        this.queueBindings = queueBindings;
    }

    public static RoutingInitializer using(ConnectionFactory connectionFactory) {
        return new RoutingInitializer(toConnectionClosure(connectionFactory));
    }

    public static RoutingInitializer using(Connection connection) {
        return new RoutingInitializer(consumer -> consumer.accept(connection));
    }

    @Override
    public void run() {
        connectionClosure.invoke(this::initializeRouting);
    }

    public RoutingInitializer addExchangeDeclaration(ExchangeDeclaration exchangeDeclaration) {
        return exchangeDeclarations(addTo(exchangeDeclarations, exchangeDeclaration));
    }

    public RoutingInitializer exchangeDeclarations(List<ExchangeDeclaration> exchangeDeclarations) {
        return new RoutingInitializer(connectionClosure, exchangeDeclarations, queueDeclarations, queueBindings);
    }

    public RoutingInitializer addQueueDeclaration(QueueDeclaration queueDeclaration) {
        return queueDeclarations(addTo(queueDeclarations, queueDeclaration));
    }

    public RoutingInitializer queueDeclarations(List<QueueDeclaration> queueDeclarations) {
        return new RoutingInitializer(connectionClosure, exchangeDeclarations, queueDeclarations, queueBindings);
    }

    public RoutingInitializer addQueueBinding(QueueBinding queueBinding) {
        return queueBindings(addTo(queueBindings, queueBinding));
    }

    public RoutingInitializer queueBindings(List<QueueBinding> queueBindings) {
        return new RoutingInitializer(connectionClosure, exchangeDeclarations, queueDeclarations, queueBindings);
    }

    private void initializeRouting(Connection connection) {
        try (Channel channel = connection.createChannel()) {
            initializeRouting(channel);
        } catch (IOException | TimeoutException e) {
            throw Throwing.propagate(e);
        }
    }

    private void initializeRouting(Channel channel) throws IOException {
        for (ExchangeDeclaration exchangeDeclaration : exchangeDeclarations) {
            channel.exchangeDeclare(
                    exchangeDeclaration.getName(),
                    exchangeDeclaration.getType(),
                    exchangeDeclaration.isDurable(),
                    exchangeDeclaration.isAutoDelete(),
                    exchangeDeclaration.getArguments());
        }

        for (QueueDeclaration queueDeclaration : queueDeclarations) {
            channel.queueDeclare(
                    queueDeclaration.getName(),
                    queueDeclaration.isDurable(),
                    queueDeclaration.isExclusive(),
                    queueDeclaration.isAutoDelete(),
                    queueDeclaration.getArguments());
        }

        for (QueueBinding queueBinding : queueBindings) {
            channel.queueBind(
                    queueBinding.getQueue(),
                    queueBinding.getExchange(),
                    queueBinding.getRoutingKey(),
                    queueBinding.getArguments());
        }
    }

    private static ConnectionClosure toConnectionClosure(ConnectionFactory connectionFactory) {
        return connectionConsumer -> {
            try (Connection connection = connectionFactory.newConnection()) {
                connectionConsumer.accept(connection);
            } catch (IOException | TimeoutException e) {
                throw Throwing.propagate(e);
            }
        };
    }

    private static <T> List<T> addTo(List<T> source, T itemToAdd) {
        List<T> destination = new ArrayList<>(source);
        destination.add(itemToAdd);
        return destination;
    }

    private interface ConnectionClosure {
        void invoke(Consumer<Connection> consumer);
    }
}
