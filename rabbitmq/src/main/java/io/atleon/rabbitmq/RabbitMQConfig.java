package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;

import java.util.List;
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

    public <T> Optional<T> load(String property, Function<? super String, T> parser) {
        return ConfigLoading.load(properties, property, parser);
    }

    public <T extends Configurable> List<T> loadListOfConfigured(String property) {
        return ConfigLoading.loadListOfConfigured(properties, property);
    }

    public <T extends Configurable> Optional<T> loadConfigured(String property) {
        return ConfigLoading.loadConfigured(properties, property);
    }

    public <T extends Configurable> T loadConfiguredOrThrow(String property) {
        return ConfigLoading.loadConfiguredOrThrow(properties, property);
    }
}
