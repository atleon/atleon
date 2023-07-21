package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public class RabbitMQConfig {

    private final Map<String, Object> properties;

    protected RabbitMQConfig(Map<String, Object> properties) {
        this.properties = properties;
    }

    public static RabbitMQConfig create(Map<String, Object> properties) {
        return new RabbitMQConfig(properties);
    }

    public ConnectionFactory buildConnectionFactory() {
        return AloConnectionFactory.from(properties);
    }

    public Map<String, Object> modifyAndGetProperties(Consumer<Map<String, Object>> modifier) {
        Map<String, Object> modifiedProperties = new HashMap<>(properties);
        modifier.accept(modifiedProperties);
        return modifiedProperties;
    }

    public <T extends Configurable> T loadConfiguredOrThrow(String property, Class<? extends T> type) {
        return ConfigLoading.loadConfiguredOrThrow(properties, property, type);
    }

    public <T extends Configurable> Optional<T> loadConfiguredWithPredefinedTypes(
        String key,
        Class<? extends T> type,
        Function<String, Optional<T>> predefinedTypeInstantiator
    ) {
        return ConfigLoading.loadConfiguredWithPredefinedTypes(properties, key, type, predefinedTypeInstantiator);
    }

    public Optional<Duration> loadDuration(String property) {
        return ConfigLoading.loadDuration(properties, property);
    }

    public Optional<Integer> loadInt(String property) {
        return ConfigLoading.loadInt(properties, property);
    }

    public <T extends Enum<T>> Optional<T> loadEnum(String property, Class<T> type) {
        return ConfigLoading.loadEnum(properties, property, type);
    }
}
