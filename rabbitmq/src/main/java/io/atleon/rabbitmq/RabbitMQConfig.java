package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import io.atleon.core.AloFactory;
import io.atleon.core.AloFactoryConfig;
import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class RabbitMQConfig {

    private final ConnectionFactory connectionFactory;

    private final Map<String, Object> properties;

    public RabbitMQConfig(ConnectionFactory connectionFactory, Map<String, Object> properties) {
        this.connectionFactory = connectionFactory;
        this.properties = properties;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public <T> AloFactory<T> loadAloFactory() {
        return AloFactoryConfig.loadDecorated(properties, AloRabbitMQMessageDecorator.class);
    }

    public <T extends Configurable> T loadConfiguredOrThrow(String property) {
        return ConfigLoading.loadConfiguredOrThrow(properties, property);
    }

    public <T> Optional<T> load(String property, Function<? super String, T> parser) {
        return ConfigLoading.load(properties, property, parser);
    }
}
