package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

@AutoConfigureStream(RabbitMQGeneration.class)
public class RabbitMQGenerationConfig implements AloStreamConfig {

    private final RabbitMQConfigSource rabbitMQConfig;

    private final String exchange;

    public RabbitMQGenerationConfig(
        @Qualifier("local") RabbitMQConfigSource rabbitMQConfig,
        @Value("${example.rabbitmq.exchange}") String exchange
    ) {
        this.rabbitMQConfig = rabbitMQConfig;
        this.exchange = exchange;
    }

    public AloRabbitMQSender<Long> buildRabbitMQLongSender() {
        RabbitMQConfigSource config = rabbitMQConfig.rename(name())
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName());
        return AloRabbitMQSender.from(config);
    }

    public String getExchange() {
        return exchange;
    }
}
