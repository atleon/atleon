package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.rabbitmq.AloRabbitMQSender;
import io.atleon.rabbitmq.DefaultRabbitMQMessageCreator;
import io.atleon.rabbitmq.LongBodySerializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.rabbitmq.RabbitMQMessageCreator;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.ConfigContext;
import org.springframework.context.annotation.Profile;

@Profile("!integrationTest")
@AutoConfigureStream(RabbitMQGenerationStream.class)
public class RabbitMQGenerationStreamConfig implements AloStreamConfig {

    private final ConfigContext context;

    public RabbitMQGenerationStreamConfig(ConfigContext context) {
        this.context = context;
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
            context.getProperty("stream.rabbitmq.input.queue")
        );
    }
}
