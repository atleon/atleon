package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.examples.spring.rabbitmq.service.NumbersService;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

@AutoConfigureStream(RabbitMQConsumptionStream.class)
public class RabbitMQConsumptionStreamConfig implements AloStreamConfig {

    private final Map<String, ?> rabbitMQProperties;

    private final String queue;

    private final NumbersService service;

    public RabbitMQConsumptionStreamConfig(
        @Qualifier("exampleRabbitMQProperties") Map<String, ?> rabbitMQProperties,
        @Value("${stream.rabbitmq.output.queue}") String queue,
        NumbersService service
    ) {
        this.rabbitMQProperties = rabbitMQProperties;
        this.queue = queue;
        this.service = service;
    }

    public AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        RabbitMQConfigSource configSource = RabbitMQConfigSource.named(name())
            .withAll(rabbitMQProperties)
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloRabbitMQReceiver.create(configSource);
    }

    public String getQueue() {
        return queue;
    }

    public NumbersService getService() {
        return service;
    }
}
