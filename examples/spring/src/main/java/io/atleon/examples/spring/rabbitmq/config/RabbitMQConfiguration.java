package io.atleon.examples.spring.rabbitmq.config;

import io.atleon.core.ConfigContext;
import io.atleon.rabbitmq.AloConnectionFactory;
import io.atleon.rabbitmq.ExchangeDeclaration;
import io.atleon.rabbitmq.QueueBinding;
import io.atleon.rabbitmq.QueueDeclaration;
import io.atleon.spring.RabbitMQRoutingInitialization;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class RabbitMQConfiguration {

    @Bean
    public RabbitMQRoutingInitialization exampleRabbitMQRoutingInitialization(
        ConfigContext context,
        @Value("${stream.rabbitmq.exchange}") String exchange,
        @Value("${stream.rabbitmq.input.queue}") String inputQueue,
        @Value("${stream.rabbitmq.output.queue}") String outputQueue
    ) {
        Map<String, String> properties = context.getPropertiesPrefixedBy("example.rabbitmq");
        return RabbitMQRoutingInitialization.using(AloConnectionFactory.create(properties))
            .addExchangeDeclaration(ExchangeDeclaration.direct(exchange))
            .addQueueDeclaration(QueueDeclaration.named(inputQueue))
            .addQueueDeclaration(QueueDeclaration.named(outputQueue))
            .addQueueBinding(QueueBinding.forQueue(inputQueue).toExchange(exchange).usingRoutingKey(inputQueue))
            .addQueueBinding(QueueBinding.forQueue(outputQueue).toExchange(exchange).usingRoutingKey(outputQueue));
    }
}
