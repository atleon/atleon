package io.atleon.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public class RabbitMQConfig {

    private final Map<String, ?> properties;

    protected RabbitMQConfig(Map<String, ?> properties) {
        this.properties = properties;
    }

    public static RabbitMQConfig create(Map<String, ?> properties) {
        return new RabbitMQConfig(properties);
    }

    public Connection buildConnection() throws IOException {
        return ConfigurableConnectionSupplier.load(properties, AloConfiguratorConnectionSupplier::new)
                .getConnection();
    }

    /**
     * @deprecated Use {@link #buildConnection()}
     */
    @Deprecated
    public ConnectionFactory buildConnectionFactory() {
        return AloConnectionFactory.create(properties);
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
            String key, Class<? extends T> type, Function<String, Optional<T>> predefinedTypeInstantiator) {
        return ConfigLoading.loadConfiguredWithPredefinedTypes(properties, key, type, predefinedTypeInstantiator);
    }

    public Optional<Duration> loadDuration(String property) {
        return ConfigLoading.loadDuration(properties, property);
    }

    public Optional<Integer> loadInt(String property) {
        return ConfigLoading.loadInt(properties, property);
    }

    public Optional<String> loadString(String property) {
        return ConfigLoading.loadString(properties, property);
    }

    public <T extends Enum<T>> Optional<T> loadEnum(String property, Class<T> type) {
        return ConfigLoading.loadEnum(properties, property, type);
    }
}
