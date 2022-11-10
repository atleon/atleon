package io.atleon.examples.spring.rabbitmq.config;

import io.atleon.amqp.embedded.EmbeddedAmqp;
import io.atleon.amqp.embedded.EmbeddedAmqpConfig;
import io.atleon.rabbitmq.ExchangeDeclaration;
import io.atleon.rabbitmq.QueueBinding;
import io.atleon.rabbitmq.QueueDeclaration;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.RabbitMQRoutingInitialization;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfiguration {

    private final EmbeddedAmqpConfig embeddedAmqpConfig = EmbeddedAmqp.start(15672);

    @Bean("local")
    public RabbitMQConfigSource localRabbitMQ() {
        return RabbitMQConfigSource.unnamed()
            .withAll(embeddedAmqpConfig.asMap());
    }

    @Bean
    public RabbitMQRoutingInitialization exampleRabbitMQRoutingInitialization(
        @Qualifier("local") RabbitMQConfigSource rabbitMQConfig,
        @Value("${example.rabbitmq.exchange}") String exchange,
        @Value("${example.rabbitmq.queue}") String queue
    ) {
        return RabbitMQRoutingInitialization.using(rabbitMQConfig.createConnectionFactoryNow())
            .addExchangeDeclaration(ExchangeDeclaration.fanout(exchange))
            .addQueueDeclaration(QueueDeclaration.named(queue))
            .addQueueBinding(QueueBinding.forQueue(queue).toExchange(exchange).usingRoutingKey(queue));
    }
}
