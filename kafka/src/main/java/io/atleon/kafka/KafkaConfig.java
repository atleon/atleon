package io.atleon.kafka;

import io.atleon.util.ConfigLoading;
import io.atleon.util.Configurable;
import org.apache.kafka.clients.CommonClientConfigs;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public class KafkaConfig {

    private final Map<String, Object> properties;

    public KafkaConfig(Map<String, Object> properties) {
        this.properties = properties;
    }

    public Map<String, Object> modifyAndGetProperties(Consumer<Map<String, Object>> modifier) {
        Map<String, Object> modifiedProperties = new HashMap<>(properties);
        modifier.accept(modifiedProperties);
        return modifiedProperties;
    }

    public String loadClientId() {
        return ConfigLoading.loadStringOrThrow(properties, CommonClientConfigs.CLIENT_ID_CONFIG);
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
}
