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

    private final Map<String, ?> properties;

    protected KafkaConfig(Map<String, ?> properties) {
        this.properties = properties;
    }

    public static KafkaConfig create(Map<String, ?> properties) {
        return new KafkaConfig(properties);
    }

    public KafkaConfig withClientIdSuffix(String delimiter, String suffix) {
        Map<String, ?> modifiedProperties = modifyAndGetProperties(it ->
            it.computeIfPresent(CommonClientConfigs.CLIENT_ID_CONFIG, (__, id) -> id + delimiter + suffix));
        return new KafkaConfig(modifiedProperties);
    }

    public Map<String, Object> nativeProperties() {
        return ConfigLoading.loadNative(properties);
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

    public Optional<String> loadString(String property) {
        return ConfigLoading.loadString(properties, property);
    }

    public <T extends Enum<T>> Optional<T> loadEnum(String property, Class<T> enumType) {
        return ConfigLoading.loadEnum(properties, property, enumType);
    }
}
