package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;

import java.util.Map;

@Profile("!integrationTest")
@AutoConfigureStream(RabbitMQGenerationStream.class)
public class RabbitMQGenerationStreamConfig implements AloStreamConfig {

    private final Map<String, ?> rabbitMQProperties;

    private final String exchange;

    private final String inputQueue;

    public RabbitMQGenerationStreamConfig(
        @Qualifier("exampleRabbitMQProperties") Map<String, ?> rabbitMQProperties,
        @Value("${stream.rabbitmq.exchange}") String exchange,
        @Value("${stream.rabbitmq.input.queue}") String inputQueue
    ) {
        this.rabbitMQProperties = rabbitMQProperties;
        this.exchange = exchange;
        this.inputQueue = inputQueue;
    }

    public AloRabbitMQSender<Long> buildRabbitMQLongSender() {
        RabbitMQConfigSource configSource = RabbitMQConfigSource.named(name())
            .withAll(rabbitMQProperties)
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName());
        return AloRabbitMQSender.create(configSource);
    }

    public RabbitMQMessageCreator<Long> buildMessageCreator() {
        return DefaultRabbitMQMessageCreator.minimalBasic(exchange, inputQueue);
    }
}
