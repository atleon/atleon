package io.atleon.core;

import java.util.HashMap;
import java.util.Map;

public final class EnvironmentalConfigs implements ConfigFunction {

    private static final String ENVIRONMENTAL_PREFIX = "io.atleon.config.";

    @Override
    public Map<String, Object> apply(String name, Map<String, Object> configs) {
        String prefix = String.format("%s.%s.", ENVIRONMENTAL_PREFIX, name);
        Map<String, Object> result = new HashMap<>(configs);
        result.putAll(ConfigLoading.loadPrefixedEnvironmentalProperties(prefix));
        return result;
    }

    @Override
    public Map<String, Object> apply(Map<String, Object> configs) {
        return configs;
    }
}
