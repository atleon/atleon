package io.atleon.kafka;

import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public class KafkaConfig {

    private final Map<String, Object> properties;

    protected KafkaConfig(Map<String, Object> properties) {
        this.properties = properties;
    }

    public static KafkaConfig create(Map<String, Object> properties) {
        return new KafkaConfig(properties);
    }

    public Map<String, Object> modifyAndGetProperties(Consumer<Map<String, Object>> modifier) {
        Map<String, Object> modifiedProperties = new HashMap<>(properties);
        modifier.accept(modifiedProperties);
        return modifiedProperties;
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

    public Optional<Boolean> loadBoolean(String property) {
        return ConfigLoading.loadBoolean(properties, property);
    }

    public Optional<Integer> loadInt(String property) {
        return ConfigLoading.loadInt(properties, property);
    }

    public Optional<Long> loadLong(String property) {
        return ConfigLoading.loadLong(properties, property);
    }

    public <T extends Enum<T>> Optional<T> loadEnum(String property, Class<T> enumType) {
        return ConfigLoading.loadEnum(properties, property, enumType);
    }
}
