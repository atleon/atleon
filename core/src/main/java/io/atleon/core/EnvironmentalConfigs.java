package io.atleon.core;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class EnvironmentalConfigs implements ConfigInterceptor {

    public static final String PREFIX = "atleon.config.";

    @Override
    public Map<String, Object> intercept(String name, Map<String, Object> configs) {
        String prefix = String.format("%s%s.", PREFIX, name);
        Map<String, Object> result = new HashMap<>(configs);
        result.putAll(loadPrefixedEnvironmentalProperties(prefix));
        return result;
    }

    @Override
    public Map<String, Object> intercept(Map<String, Object> configs) {
        return configs;
    }

    private static Map<String, Object> loadPrefixedEnvironmentalProperties(String prefix) {
        Map<String, Object> properties = new HashMap<>();
        consumePrefixed(System.getenv(), key -> key.replaceAll("_", ".").toLowerCase(), prefix, properties::put);
        consumePrefixed(System.getProperties(), Object::toString, prefix, properties::put);
        return properties;
    }

    private static <K> void consumePrefixed(
        Map<K, ?> source,
        Function<K, String> keyToPropertyKey,
        String prefix,
        BiConsumer<String, Object> consumer
    ) {
        source.forEach((key, value) -> {
            String propertyKey = keyToPropertyKey.apply(key);
            if (propertyKey.startsWith(prefix)) {
                consumer.accept(propertyKey.substring(prefix.length()), value);
            }
        });
    }
}
