package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStreamConfig;
import org.springframework.context.annotation.Profile;

@Profile("!integrationTest")
@AutoConfigureStream(RabbitMQGenerationStream.class)
public class RabbitMQGenerationStreamConfig extends SpringAloStreamConfig {

    public AloRabbitMQSender<Long> buildRabbitMQLongSender() {
        RabbitMQConfigSource configSource = getBean("exampleRabbitMQConfigSource", RabbitMQConfigSource.class)
            .rename(name())
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, LongBodySerializer.class.getName());
        return AloRabbitMQSender.create(configSource);
    }

    public RabbitMQMessageCreator<Long> buildLongMessageCreator() {
        return DefaultRabbitMQMessageCreator.minimalBasic(
            getProperty("stream.rabbitmq.exchange"),
            getProperty("stream.rabbitmq.input.queue")
        );
    }
}
