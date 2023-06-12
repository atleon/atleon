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

@AutoConfigureStream(RabbitMQProcessingStream.class)
public class RabbitMQProcessingStreamConfig implements AloStreamConfig {

    private final Map<String, ?> rabbitMQProperties;

    private final String queue;

    private final NumbersService numbersService;

    public RabbitMQProcessingStreamConfig(
        @Qualifier("exampleRabbitMQProperties") Map<String, ?> rabbitMQProperties,
        @Value("${stream.rabbitmq.input.queue}") String queue,
        NumbersService numbersService
    ) {
        this.rabbitMQProperties = rabbitMQProperties;
        this.queue = queue;
        this.numbersService = numbersService;
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

    public NumbersService getNumbersService() {
        return numbersService;
    }
}
