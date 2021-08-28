package io.atleon.rabbitmq;

import java.util.Map;

public final class TestRabbitMQSourceFactory {

    private TestRabbitMQSourceFactory() {

    }

    public static RabbitMQConfigSource createStringSource(Map<String, ?> configs) {
        return new RabbitMQConfigSource().withAll(configs)
            .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
            .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, StringBodyDeserializer.class.getName());
    }
}
