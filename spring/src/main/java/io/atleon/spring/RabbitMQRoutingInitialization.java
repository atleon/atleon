package io.atleon.spring;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.atleon.rabbitmq.ExchangeDeclaration;
import io.atleon.rabbitmq.QueueBinding;
import io.atleon.rabbitmq.QueueDeclaration;
import io.atleon.rabbitmq.RoutingInitializer;
import java.util.List;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.Ordered;

public class RabbitMQRoutingInitialization implements ApplicationListener<ApplicationContextEvent>, Ordered {

    private final RoutingInitializer routingInitializer;

    public RabbitMQRoutingInitialization(RoutingInitializer routingInitializer) {
        this.routingInitializer = routingInitializer;
    }

    public static RabbitMQRoutingInitialization using(ConnectionFactory connectionFactory) {
        return new RabbitMQRoutingInitialization(RoutingInitializer.using(connectionFactory));
    }

    public static RabbitMQRoutingInitialization using(Connection connection) {
        return new RabbitMQRoutingInitialization(RoutingInitializer.using(connection));
    }

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            routingInitializer.run();
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    public RabbitMQRoutingInitialization addExchangeDeclaration(ExchangeDeclaration exchangeDeclaration) {
        return new RabbitMQRoutingInitialization(routingInitializer.addExchangeDeclaration(exchangeDeclaration));
    }

    public RabbitMQRoutingInitialization exchangeDeclarations(List<ExchangeDeclaration> exchangeDeclarations) {
        return new RabbitMQRoutingInitialization(routingInitializer.exchangeDeclarations(exchangeDeclarations));
    }

    public RabbitMQRoutingInitialization addQueueDeclaration(QueueDeclaration queueDeclaration) {
        return new RabbitMQRoutingInitialization(routingInitializer.addQueueDeclaration(queueDeclaration));
    }

    public RabbitMQRoutingInitialization queueDeclarations(List<QueueDeclaration> queueDeclarations) {
        return new RabbitMQRoutingInitialization(routingInitializer.queueDeclarations(queueDeclarations));
    }

    public RabbitMQRoutingInitialization addQueueBinding(QueueBinding queueBinding) {
        return new RabbitMQRoutingInitialization(routingInitializer.addQueueBinding(queueBinding));
    }

    public RabbitMQRoutingInitialization queueBindings(List<QueueBinding> queueBindings) {
        return new RabbitMQRoutingInitialization(routingInitializer.queueBindings(queueBindings));
    }
}
