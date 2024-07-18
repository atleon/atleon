package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.examples.spring.rabbitmq.service.NumbersService;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import io.atleon.spring.AutoConfigureStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

@AutoConfigureStream(RabbitMQProcessingStream.class)
public class RabbitMQProcessingStreamConfig implements AloStreamConfig {

    private final Map<String, ?> rabbitMQProperties;

    private final String exchange;

    private final String inputQueue;

    private final String outputQueue;

    private final NumbersService service;

    public RabbitMQProcessingStreamConfig(
        @Qualifier("exampleRabbitMQProperties") Map<String, ?> rabbitMQProperties,
        @Value("${stream.rabbitmq.exchange}") String exchange,
        @Value("${stream.rabbitmq.input.queue}") String inputQueue,
        @Value("${stream.rabbitmq.output.queue}") String outputQueue,
        NumbersService service
    ) {
        this.rabbitMQProperties = rabbitMQProperties;
        this.exchange = exchange;
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.service = service;
    }

    public AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        RabbitMQConfigSource configSource = baseRabbitMQConfigSource()
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloRabbitMQReceiver.create(configSource);
    }

    public AloRabbitMQSender<Long> buildRabbitMQLongSender() {
        RabbitMQConfigSource configSource = baseRabbitMQConfigSource()
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName());
        return AloRabbitMQSender.create(configSource);
    }

    public RabbitMQMessageCreator<Long> buildLongMessageCreator() {
        return DefaultRabbitMQMessageCreator.minimalBasic(exchange, outputQueue);
    }

    private RabbitMQConfigSource baseRabbitMQConfigSource() {
        return RabbitMQConfigSource.named(name())
            .withAll(rabbitMQProperties);
    }

    public String getInputQueue() {
        return inputQueue;
    }

    public NumbersService getService() {
        return service;
    }
}
