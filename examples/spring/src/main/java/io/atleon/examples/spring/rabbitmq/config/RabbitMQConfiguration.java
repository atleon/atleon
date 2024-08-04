package io.atleon.examples.spring.rabbitmq.config;

import io.atleon.rabbitmq.ExchangeDeclaration;
import io.atleon.rabbitmq.QueueBinding;
import io.atleon.rabbitmq.QueueDeclaration;
import io.atleon.rabbitmq.RabbitMQConfig;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.RabbitMQRoutingInitialization;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfiguration {

    @Bean
    public RabbitMQRoutingInitialization exampleRabbitMQRoutingInitialization(
        RabbitMQConfigSource exampleRabbitMQConfigSource,
        @Value("${stream.rabbitmq.exchange}") String exchange,
        @Value("${stream.rabbitmq.input.queue}") String inputQueue,
        @Value("${stream.rabbitmq.output.queue}") String outputQueue
    ) {
        RabbitMQConfig config = exampleRabbitMQConfigSource.create().block();
        return RabbitMQRoutingInitialization.using(config.buildConnectionFactory())
            .addExchangeDeclaration(ExchangeDeclaration.direct(exchange))
            .addQueueDeclaration(QueueDeclaration.named(inputQueue))
            .addQueueDeclaration(QueueDeclaration.named(outputQueue))
            .addQueueBinding(QueueBinding.forQueue(inputQueue).toExchange(exchange).usingRoutingKey(inputQueue))
            .addQueueBinding(QueueBinding.forQueue(outputQueue).toExchange(exchange).usingRoutingKey(outputQueue));
    }
}
