package io.atleon.examples.spring.rabbitmq.stream;

import io.atleon.examples.spring.rabbitmq.service.NumbersService;
import io.atleon.rabbitmq.AloRabbitMQReceiver;
import io.atleon.rabbitmq.LongBodyDeserializer;
import io.atleon.rabbitmq.RabbitMQConfigSource;
import io.atleon.spring.AutoConfigureStream;
import io.atleon.spring.SpringAloStreamConfig;

@AutoConfigureStream(RabbitMQConsumptionStream.class)
public class RabbitMQConsumptionStreamConfig extends SpringAloStreamConfig {

    public AloRabbitMQReceiver<Long> buildRabbitMQLongReceiver() {
        RabbitMQConfigSource configSource = getBean("exampleRabbitMQConfigSource", RabbitMQConfigSource.class)
            .rename(name())
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, LongBodyDeserializer.class.getName());
        return AloRabbitMQReceiver.create(configSource);
    }

    public String getQueue() {
        return getProperty("stream.rabbitmq.output.queue");
    }

    public NumbersService getService() {
        return getBean(NumbersService.class);
    }
}
