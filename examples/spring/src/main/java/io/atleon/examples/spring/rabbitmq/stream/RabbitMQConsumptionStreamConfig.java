package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.core.AloStreamConfig;
import io.atleon.core.ConfigContext;
import io.atleon.examples.spring.rabbitmq.service.NumbersService;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.AutoConfigureStream;

@AutoConfigureStream(RabbitMQConsumptionStream.class)
public class RabbitMQConsumptionStreamConfig implements AloStreamConfig {

    private final ConfigContext context;

    public RabbitMQConsumptionStreamConfig(ConfigContext context) {
        this.context = context;
    }

    public AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        RabbitMQConfigSource configSource = RabbitMQConfigSource.named(name())
            .withAll(context.getPropertiesPrefixedBy("example.rabbitmq"))
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloRabbitMQReceiver.create(configSource);
    }

    public String getQueue() {
        return context.getProperty("stream.rabbitmq.output.queue");
    }

    public NumbersService getService() {
        return context.getBean(NumbersService.class);
    }
}
