package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.util.function.Consumer;

@AutoConfigureStream(RabbitMQProcessing.class)
public class RabbitMQProcessingConfig implements AloStreamConfig {

    private final RabbitMQConfigSource rabbitMQConfig;

    private final String queue;

    public RabbitMQProcessingConfig(
        @Qualifier("local") RabbitMQConfigSource rabbitMQConfig,
        @Value("${example.rabbitmq.queue}") String queue
    ) {
        this.rabbitMQConfig = rabbitMQConfig;
        this.queue = queue;
    }

    public AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        RabbitMQConfigSource config = rabbitMQConfig.rename(name())
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloRabbitMQReceiver.from(config);
    }

    public String getQueue() {
        return queue;
    }

    public Consumer<String> getConsumer() {
        return System.out::println;
    }
}
