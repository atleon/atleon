package io.atleon.examples.spring.rabbitmq.config;

import io.atleon.rabbitmq.ExchangeDeclaration;
import io.atleon.rabbitmq.QueueBinding;
import io.atleon.rabbitmq.QueueDeclaration;
import io.atleon.rabbitmq.RabbitMQConfigSource;
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

    @Bean("exampleRabbitMQConfigSource")
    public RabbitMQConfigSource exampleRabbitMQConfigSource(
        @Qualifier("exampleRabbitMQProperties") Map<String, ?> rabbitMQProperties
    ) {
        return RabbitMQConfigSource.unnamed().withAll(rabbitMQProperties);
    }

    @Bean
    public RabbitMQRoutingInitialization exampleRabbitMQRoutingInitialization(
        @Qualifier("exampleRabbitMQConfigSource") RabbitMQConfigSource configSource,
        @Value("${stream.rabbitmq.exchange}") String exchange,
        @Value("${stream.rabbitmq.input.queue}") String inputQueue,
        @Value("${stream.rabbitmq.output.queue}") String outputQueue
    ) {
        return RabbitMQRoutingInitialization.using(configSource.createConnectionFactoryNow())
            .addExchangeDeclaration(ExchangeDeclaration.direct(exchange))
            .addQueueDeclaration(QueueDeclaration.named(inputQueue))
            .addQueueDeclaration(QueueDeclaration.named(outputQueue))
            .addQueueBinding(QueueBinding.forQueue(inputQueue).toExchange(exchange).usingRoutingKey(inputQueue))
            .addQueueBinding(QueueBinding.forQueue(outputQueue).toExchange(exchange).usingRoutingKey(outputQueue));
    }
}
