package io.atleon.rabbitmq;

import java.util.Map;

public final class TestRabbitMQSourceFactory {

    private TestRabbitMQSourceFactory() {

    }

    public static RabbitMQConfigSource createStringSource(Map<String, ?> configs) {
        RabbitMQConfigSource configSource = new RabbitMQConfigSource();
        configs.forEach(configSource::put);
        configSource.put(AloRabbitMQSender.BODY_SERIALIZER_CONFIG, StringBodySerializer.class.getName());
        configSource.put(AloRabbitMQReceiver.BODY_DESERIALIZER_CONFIG, StringBodyDeserializer.class.getName());
        return configSource;
    }
}
