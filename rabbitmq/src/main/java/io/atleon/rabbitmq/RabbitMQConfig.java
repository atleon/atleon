package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import io.atleon.util.ConfigLoading;
import io.atleon.util.Instantiation;
import io.atleon.util.TypeResolution;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        List<T> listOfConfigured = ConfigLoading
            .loadCollection(properties, property, Instantiation::<T>one, Collectors.toList())
            .orElseGet(Collections::emptyList);
        listOfConfigured.forEach(interceptor -> interceptor.configure(properties));
        return listOfConfigured;
    }

    public <T extends Configurable> Optional<T> loadConfigured(String property) {
        return ConfigLoading.<Class<? extends T>>load(properties, property, TypeResolution::classForQualifiedName)
            .map(this::createConfigured);
    }

    public <T extends Configurable> T loadConfiguredOrThrow(String property) {
        return createConfigured(ConfigLoading.loadOrThrow(properties, property, TypeResolution::classForQualifiedName));
    }

    private <T extends Configurable> T createConfigured(Class<? extends T> configurableClass) {
        T configurable = Instantiation.one(configurableClass);
        configurable.configure(properties);
        return configurable;
    }
}
