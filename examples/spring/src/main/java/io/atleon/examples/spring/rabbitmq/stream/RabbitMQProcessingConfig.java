package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;
import java.util.function.Consumer;

@AutoConfigureStream(RabbitMQProcessing.class)
public class RabbitMQProcessingConfig implements AloStreamConfig {

    private final Map<String, ?> rabbitMQProperties;

    private final String queue;

    public RabbitMQProcessingConfig(
        @Value("#{localRabbitMQ}") Map<String, ?> rabbitMQProperties,
        @Value("${example.rabbitmq.queue}") String queue
    ) {
        this.rabbitMQProperties = rabbitMQProperties;
        this.queue = queue;
    }

    public AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        RabbitMQConfigSource configSource = RabbitMQConfigSource.named(name())
            .withAll(rabbitMQProperties)
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloRabbitMQReceiver.from(configSource);
    }

    public String getQueue() {
        return queue;
    }

    public Consumer<String> getConsumer() {
        return System.out::println;
    }
}
