package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

@AutoConfigureStream(RabbitMQGeneration.class)
public class RabbitMQGenerationConfig implements AloStreamConfig {

    private final Map<String, ?> rabbitMQProperties;

    private final String exchange;

    public RabbitMQGenerationConfig(
        @Value("#{localRabbitMQ}") Map<String, ?> rabbitMQProperties,
        @Value("${example.rabbitmq.exchange}") String exchange
    ) {
        this.rabbitMQProperties = rabbitMQProperties;
        this.exchange = exchange;
    }

    public AloRabbitMQSender<Long> buildRabbitMQLongSender() {
        RabbitMQConfigSource configSource = RabbitMQConfigSource.named(name())
            .withAll(rabbitMQProperties)
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName());
        return AloRabbitMQSender.from(configSource);
    }

    public String getExchange() {
        return exchange;
    }
}
