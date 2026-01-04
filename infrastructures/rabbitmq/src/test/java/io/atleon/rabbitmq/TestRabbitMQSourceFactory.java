package io.atleon.rabbitmq;

import io.atleon.amqp.embedded.EmbeddedAmqpConfig;

public final class TestRabbitMQSourceFactory {

    private TestRabbitMQSourceFactory() {}

    public static RabbitMQConfigSource createStringSource(EmbeddedAmqpConfig config) {
        return RabbitMQConfigSource.named("test-rabbitmq")
                .withAll(config.asMap())
                .with(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName())
                .with(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, StringBodyDeserializer.class.getName());
    }
}
