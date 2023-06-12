package io.atleon.examples.spring.rabbitmq.config;

import io.atleon.rabbitmq.AloConnectionFactory;
import io.atleon.rabbitmq.ExchangeDeclaration;
import io.atleon.rabbitmq.QueueBinding;
import io.atleon.rabbitmq.QueueDeclaration;
import io.atleon.spring.RabbitMQRoutingInitialization;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMQConfiguration {

    @Bean("exampleRabbitMQProperties")
    @ConfigurationProperties(prefix = "example.rabbitmq")
    public Map<String, String> exampleRabbitMQProperties() {
        return new HashMap<>();
    }

    @Bean
    public RabbitMQRoutingInitialization exampleRabbitMQRoutingInitialization(
        @Qualifier("exampleRabbitMQProperties") Map<String, ?> rabbitMQProperties,
        @Value("${stream.rabbitmq.exchange}") String exchange,
        @Value("${stream.rabbitmq.input.queue}") String queue
    ) {
        return RabbitMQRoutingInitialization.using(AloConnectionFactory.from(rabbitMQProperties))
            .addExchangeDeclaration(ExchangeDeclaration.direct(exchange))
            .addQueueDeclaration(QueueDeclaration.named(queue))
            .addQueueBinding(QueueBinding.forQueue(queue).toExchange(exchange).usingRoutingKey(queue));
    }
}
