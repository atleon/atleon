package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.examples.spring.rabbitmq.service.NumbersService;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStreamConfig;

@AutoConfigureStream(RabbitMQProcessingStream.class)
public class RabbitMQProcessingStreamConfig extends SpringAloStreamConfig {

    public AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        RabbitMQConfigSource configSource = getBean("exampleRabbitMQConfigSource", RabbitMQConfigSource.class)
            .rename(name())
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloRabbitMQReceiver.create(configSource);
    }

    public AloRabbitMQSender<Long> buildRabbitMQLongSender() {
        RabbitMQConfigSource configSource = getBean("exampleRabbitMQConfigSource", RabbitMQConfigSource.class)
            .rename(name())
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName());
        return AloRabbitMQSender.create(configSource);
    }

    public RabbitMQMessageCreator<Long> buildLongMessageCreator() {
        return DefaultRabbitMQMessageCreator.minimalBasic(
            getProperty("stream.rabbitmq.exchange"),
            getProperty("stream.rabbitmq.output.queue")
        );
    }

    public String getInputQueue() {
        return getProperty("stream.rabbitmq.input.queue");
    }

    public NumbersService getService() {
        return getBean(NumbersService.class);
    }
}
