package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.core.ConfigContext;
import io.atleon.examples.spring.rabbitmq.service.NumbersService;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import io.atleon.spring.AutoConfigureStream;

@AutoConfigureStream(RabbitMQProcessingStream.class)
public class RabbitMQProcessingStreamConfig implements AloStreamConfig {

    private final ConfigContext context;

    public RabbitMQProcessingStreamConfig(ConfigContext context) {
        this.context = context;
    }

    public AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        RabbitMQConfigSource configSource = RabbitMQConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.rabbitmq"))
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloRabbitMQReceiver.create(configSource);
    }

    public AloRabbitMQSender<Long> buildRabbitMQLongSender() {
        RabbitMQConfigSource configSource = RabbitMQConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.rabbitmq"))
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName());
        return AloRabbitMQSender.create(configSource);
    }

    public RabbitMQMessageCreator<Long> buildLongMessageCreator() {
        return DefaultRabbitMQMessageCreator.minimalBasic(
            context.getProperty("stream.rabbitmq.exchange"),
            context.getProperty("stream.rabbitmq.output.queue")
        );
    }

    public String getInputQueue() {
        return context.getProperty("stream.rabbitmq.input.queue");
    }

    public NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
